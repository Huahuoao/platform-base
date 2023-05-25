package xyz.eulix.platform.services.lock.repository;

import io.quarkus.hibernate.orm.panache.PanacheRepository;
import xyz.eulix.platform.services.lock.entity.DistributedLockEntity;

import javax.enterprise.context.ApplicationScoped;
import javax.transaction.Transactional;
import java.time.LocalDateTime;
import java.util.List;

@ApplicationScoped
public class DistributedEntityRepository implements PanacheRepository<DistributedLockEntity> {
    public DistributedLockEntity getLock(String taskName) {
        return find("task_name", taskName).firstResult();
    }

    public List<DistributedLockEntity> getLockReadWrite(String taskName) {
        return find("task_name", taskName).list();
    }
    public DistributedLockEntity getLock(String taskName,String uuid) {
        return find("task_name=?1 and lock_key = ?2", taskName,uuid).firstResult();
    }

    @Transactional
    public void deleteLock(Long id) {
        this.delete("id", id);
    }

    @Transactional
    public void updateLockAndTime(DistributedLockEntity entity) {
        this.update("expiration_time = ?1,lock_num=?2 where id =?3", entity.getExpirationTime(), entity.getLockNum(), entity.getId());
    }

    @Transactional
    public void updateLockNum(DistributedLockEntity entity) {
        this.update("lock_num=?1 where id =?2", entity.getLockNum(), entity.getId());
    }

}
