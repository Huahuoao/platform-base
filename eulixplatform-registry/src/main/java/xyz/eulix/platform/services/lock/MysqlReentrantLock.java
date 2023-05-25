package xyz.eulix.platform.services.lock;

import io.quarkus.arc.Arc;
import org.jboss.logging.Logger;
import xyz.eulix.platform.services.config.ApplicationProperties;
import xyz.eulix.platform.services.lock.entity.DistributedLockEntity;
import xyz.eulix.platform.services.lock.service.MysqlLockService;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class MysqlReentrantLock implements DistributedLock {

    private static final Logger LOG = Logger.getLogger("app.log");

    private String keyName;
    private String lockValue;
    private Integer timeout;


    private MysqlLockService service;

    public MysqlReentrantLock(String keyName, String lockValue, Integer timeout) {
        this.keyName = keyName;
        this.lockValue = lockValue;
        this.timeout = timeout * 1000;
        this.service = Arc.container().instance(MysqlLockService.class).get();
    }

    public MysqlReentrantLock() {

    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {

        long start = System.currentTimeMillis();
        long end;
        long sleepTime = 1L; // 重试间隔时间，单位ms。指数增长，最大值为1024ms
        do {

            //尝试获取锁
            boolean success = tryLock(keyName, lockValue);
            if (success) {
                //成功获取锁，返回
                LOG.debugv("acquire lock success, keyName:{0}", keyName);
                return true;
            }
            // 等待后继续尝试获取
            if (sleepTime < 1000L) {
                sleepTime = sleepTime << 1;
            }
            LOG.debugv("acquire lock fail, retry after: {0}ms", sleepTime);
            Thread.sleep(sleepTime);
            end = System.currentTimeMillis();
        } while (end - start < unit.toMillis(waitTime));
        LOG.debugv("acquire lock timeout, elapsed: {0}ms", System.currentTimeMillis() - start);
        return false;
    }

    @Override
    public boolean tryLock() throws InterruptedException {
        return tryLock(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void unlock() {
        unlock(keyName, lockValue);
    }

    public void unlock(String taskName, String uuid) {
        Integer result = service.unlock(taskName, uuid);
        MysqlReadWriteLock.getUnlockResult(taskName, uuid, result, LOG);
    }

    public boolean tryLock(String id, String uuid) throws InterruptedException {
        return service.tryLock(id, uuid);


    }

}
