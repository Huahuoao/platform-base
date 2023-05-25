/*
 * Copyright (c) 2022 Institute of Software Chinese Academy of Sciences (ISCAS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package xyz.eulix.platform.services.lock;

import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import xyz.eulix.platform.services.lock.service.MysqlLockService;

import javax.inject.Inject;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@QuarkusTest
public class DistributedLockTest {
    private static final Logger LOG = Logger.getLogger("app.log");
    @Inject
    private MysqlLockService mysqlLockService;

    @Inject
    DistributedLockFactory lockFactory;

    @Test
    void testRedisReentrantLock() throws InterruptedException {
        String keyName = "RedisReentrantLock";
        DistributedLock lock = lockFactory.newRedisReentrantLock(keyName);
        // 加锁
        Boolean isLocked = lock.tryLock();
        if (isLocked) {
            LOG.infov("acquire lock success, keyName:{0}", keyName);
            try {
                if (lock.tryLock()) {
                    // 这里写需要处理业务的业务代码
                    LOG.infov("reentrant lock success, keyName:{0}", keyName);
                    LOG.info("do something.");
                    Thread.sleep(3000);
                }
            } finally {
                // 释放锁
                lock.unlock();
                lock.unlock();
                LOG.infov("release lock success, keyName:{0}", keyName);
            }
        } else {
            LOG.infov("acquire lock fail, keyName:{0}", keyName);
        }
        Assertions.assertTrue(isLocked);
    }


    @Test
    void testRedisReadWriteLock() throws InterruptedException {
        String keyName = "RedisReadWriteLock";
        DistributedLock lock = lockFactory.newRedisReadWriteLock(keyName, "write");
        // 加锁
        Boolean isLocked = lock.tryLock();
        if (isLocked) {
            LOG.infov("acquire lock success, keyName:{0}", keyName);
            try {
                if (lock.tryLock()) {
                    // 这里写需要处理业务的业务代码
                    LOG.infov("reentrant lock success, keyName:{0}", keyName);
                    LOG.info("do something.");
                    Thread.sleep(3000);
                }
            } finally {
                // 释放锁
                lock.unlock();
                lock.unlock();
                LOG.infov("release lock success, keyName:{0}", keyName);
            }
        } else {
            LOG.infov("acquire lock fail, keyName:{0}", keyName);
        }
        Assertions.assertTrue(isLocked);
    }

    @Test
    void testMysqlReentrantLock() throws InterruptedException {
        String keyName = "MysqlReentrantLock";
        DistributedLock lock = lockFactory.newMysqlReentrantLock(keyName);
        // 加锁
        Boolean isLocked = lock.tryLock();
        if (isLocked) {
            LOG.infov("acquire lock success, keyName:{0}", keyName);
            try {
                if (lock.tryLock()) {
                    // 这里写需要处理业务的业务代码
                    LOG.infov("reentrant lock success, keyName:{0}", keyName);
                    LOG.info("do something.");
                    Thread.sleep(3000);
                }
            } finally {
                // 释放锁
                lock.unlock();
                lock.unlock();
                LOG.infov("release lock success, keyName:{0}", keyName);
            }
        } else {
            LOG.infov("acquire lock fail, keyName:{0}", keyName);
        }
        Assertions.assertTrue(isLocked);
    }

    @Test
    void testMysqlReadWriteLock() throws InterruptedException {
        String keyName = "MysqlReadWriteLock";
        DistributedLock lock = lockFactory.newMysqlReadWriteLock(keyName, "write");
        // 加锁
        Boolean isLocked = lock.tryLock();
        if (isLocked) {
            LOG.infov("acquire lock success, keyName:{0}", keyName);
            try {
                if (lock.tryLock()) {
                    // 这里写需要处理业务的业务代码
                    LOG.infov("reentrant lock success, keyName:{0}", keyName);
                    LOG.info("do something.");
                    Thread.sleep(3000);
                }
            } finally {
                // 释放锁
                lock.unlock();
                lock.unlock();
                LOG.infov("release lock success, keyName:{0}", keyName);
            }
        } else {
            LOG.infov("acquire lock fail, keyName:{0}", keyName);
        }
        Assertions.assertTrue(isLocked);
    }

    @Test
    void lockEfficiencyTest() throws InterruptedException { //6ms
        AtomicLong lockTime = new AtomicLong();
        AtomicLong unLockTime = new AtomicLong();
        for (int i = 0; i < 24; i++) {
            new Thread(() -> {
                DistributedLock lock =  lockFactory.newRedisReadWriteLock(String.valueOf(UUID.randomUUID()),"write");;
                try {
                    long t1 = System.currentTimeMillis();
                   lock.tryLock();
                    //lock.tryLock();
                    lockTime.addAndGet(System.currentTimeMillis() - t1);
                } catch (Exception e) {
                } finally {
                    long t2 = System.currentTimeMillis();
                  // lock.unlock();
                   lock.unlock();
                    unLockTime.addAndGet(System.currentTimeMillis() - t2);
                }
            }).start();
        }
        Thread.sleep(2000);
        System.out.println("加锁平均用时: "+lockTime.longValue()/24+"ms");
        System.out.println("解锁平均用时: "+unLockTime.longValue()/24+"ms");
    }

}
