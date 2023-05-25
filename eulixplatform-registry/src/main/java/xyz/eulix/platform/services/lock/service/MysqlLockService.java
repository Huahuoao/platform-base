package xyz.eulix.platform.services.lock.service;

import xyz.eulix.platform.services.config.ApplicationProperties;
import xyz.eulix.platform.services.lock.entity.DistributedLockEntity;
import xyz.eulix.platform.services.lock.repository.DistributedEntityRepository;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@ApplicationScoped
public class MysqlLockService {
    @Inject
    DistributedEntityRepository repository;
    @Inject
    ApplicationProperties applicationProperties;

    @Transactional
    public DistributedLockEntity getLock(String taskName) {
        return repository.getLock(taskName);
    }

    @Transactional
    public List<DistributedLockEntity> getLockReadWrite(String taskName) {
        return repository.getLockReadWrite(taskName);
    }

    @Transactional
    public DistributedLockEntity getLock(String id, String uuid) {
        return repository.getLock(id, uuid);
    }


    @Transactional
    public void newLock(String key, String uuid) {
        DistributedLockEntity distributedLockEntity = new DistributedLockEntity();
        distributedLockEntity.setLockNum(1);
        distributedLockEntity.setLockKey(uuid);
        distributedLockEntity.setTaskName(key);
        distributedLockEntity.setExpirationTime(LocalDateTime.now().plusSeconds(applicationProperties.getLockExpireTime()));
        repository.persistAndFlush(distributedLockEntity);
    }


    @Transactional
    public boolean isExpired(LocalDateTime time) {
        LocalDateTime now = LocalDateTime.now();
        int result = now.compareTo(time);
        return result >= 0;   //true表示过期了
    }

    @Transactional
    public void deleteLock(Long id) {
        repository.deleteLock(id);
    }


    @Transactional
    public boolean tryLock(String taskName, String uuid) throws InterruptedException {
        DistributedLockEntity lock = this.getLock(taskName);

        if (lock == null) {
            this.newLock(taskName, uuid);
            // 创建锁成功
            return true;
        }

        boolean expired = this.isExpired(lock.getExpirationTime());
        if (expired) {
            this.deleteLock(lock.getId());
            this.newLock(taskName, uuid);
            // 锁过期，删除锁，创建新锁
            return true;
        } else {
            // 锁没过期
            if (uuid.equals(lock.getLockKey())) {
                // 是同一个进程的锁，开始重入
                lock.setLockNum(lock.getLockNum() + 1);
                lock.setExpirationTime(LocalDateTime.now().plusSeconds(applicationProperties.getLockExpireTime()));
                repository.updateLockAndTime(lock);
                return true;
            } else {
                // 失败重试
                return false;
            }
        }
    }


    @Transactional
    public boolean tryLock(String taskName, String uuid, String mode) throws InterruptedException {
        if (mode.equals("read")) {   //如果是读锁
            DistributedLockEntity writeLock = this.getLock("write-" + taskName);
            if (writeLock != null) { //如果写锁不为空
                boolean expired = this.isExpired(writeLock.getExpirationTime());
                if (expired) {
                    this.deleteLock(writeLock.getId());
                }//写锁过期删了
                else {
                    return false;
                }
            }

            DistributedLockEntity lock = this.getLock("read-" + taskName, uuid);  //获取读锁，如果获取到就是重入锁，不是就创建新锁
            if (lock != null) {
                //判断过期
                if (this.isExpired(lock.getExpirationTime())) {
                    this.deleteLock(lock.getId());  //过期删掉
                } else { //没过期就重入
                    lock.setLockNum(lock.getLockNum() + 1);
                    lock.setExpirationTime(LocalDateTime.now().plusSeconds(applicationProperties.getLockExpireTime()));
                    repository.updateLockAndTime(lock);
                    return true;
                }
            } else {
                this.newLock("read-" + taskName, uuid);
                return true;
            }
        } else if (mode.equals("write")) {  //进入写锁逻辑
            List<DistributedLockEntity> writeLocks = this.getLockReadWrite("read-" + taskName); //判断是否有读锁
            if (writeLocks != null) {
                for (DistributedLockEntity writeLock : writeLocks) {  //遍历获取到读读锁
                    if (this.isExpired(writeLock.getExpirationTime())) {
                        this.deleteLock(writeLock.getId());  //如果过期就删掉
                    } else {
                        return false; //没过期就false,但凡有一个没过期，都会失败。
                    }
                }
            }
            //这边就是没有读锁了
            DistributedLockEntity lock = this.getLock("write-" + taskName);  //获取写锁
            if (lock != null) { //获取到了就要判断过期
                if (this.isExpired(lock.getExpirationTime())) {
                    this.deleteLock(lock.getId());
                    this.newLock("write-" + taskName, uuid);
                    return true;
                } else {
                    //判断是否重入
                    if (uuid.equals(lock.getLockKey())) {
                        //重入
                        lock.setLockNum(lock.getLockNum() + 1);
                        lock.setExpirationTime(LocalDateTime.now().plusSeconds(applicationProperties.getLockExpireTime()));
                        repository.updateLockAndTime(lock);
                        return true;
                    }
                    return false;
                }
            } else {
                this.newLock("write-" + taskName, uuid);
                return true;
            }
        } else {
            throw new RuntimeException("Lock mode is incorrect, expected value is \"read\" or \"write\"");
        }
        return false;
    }


    @Transactional
    public DistributedLockEntity check() {
        return this.getLock("test");
    }
    @Transactional
    public void initTest() {
        DistributedLockEntity lock = this.getLock("test");
        if(lock==null)
        {
            DistributedLockEntity entity = new DistributedLockEntity();
            entity.setLockNum(10);
            entity.setTaskName("test");
            repository.persist(entity);
            return;
        }
        lock.setLockNum(10);
        repository.updateLockNum(lock);
    }
    @Transactional
    public void buy(DistributedLockEntity entity) {
        repository.updateLockNum(entity);
    }

    @Transactional
    public int unlock(String taskName, String uuid) {
        DistributedLockEntity lock = repository.getLock(taskName,uuid);
        return notHoldLock(lock);
    }

    private int notHoldLock(DistributedLockEntity lock) {
        if (lock == null) {
            return 2; // 异常：该线程没有锁或进程不匹配
        } else {
            if (this.isExpired(lock.getExpirationTime()) || lock.getLockNum() == 1) {
                repository.deleteLock(lock.getId());
            } else {
                lock.setLockNum(lock.getLockNum() - 1);
                repository.updateLockNum(lock);
                return 0; // 减少重入成功
            }
        }
        return 1; // 解锁成功
    }

    @Transactional
    public int unlock(String taskName, String uuid,String mode) {
        DistributedLockEntity lock = repository.getLock(mode+"-"+taskName,uuid);
        return notHoldLock(lock);
    }


}