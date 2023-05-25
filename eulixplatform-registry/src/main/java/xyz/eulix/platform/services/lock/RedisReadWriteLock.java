package xyz.eulix.platform.services.lock;

import io.quarkus.logging.Log;
import io.quarkus.redis.client.RedisClient;
import io.vertx.redis.client.Response;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class RedisReadWriteLock implements DistributedLock {
    private static final Logger LOG = Logger.getLogger("app.log");

    private RedisClient redisClient;

    private String keyName;

    private String lockValue;

    private Integer timeout;    //锁超时时间
    private String mode;

    public RedisReadWriteLock() {
    }

    public RedisReadWriteLock(RedisClient redisClient, String keyName, String lockValue, Integer timeout, String mode) {
        this.redisClient = redisClient;
        this.keyName = keyName;
        this.lockValue = lockValue;
        this.mode = mode;
        this.timeout = timeout * 1000;
    }

    /**
     *
     * @param waitTime 等待时间
     * @param unit 单位
     * @return
     * @throws InterruptedException
     * 重复尝试获取锁
     */
    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        long start = System.currentTimeMillis();
        long end;
        long sleepTime = 1L; // 重试间隔时间，单位ms。指数增长，最大值为1024ms
        do {
            //尝试获取锁
            boolean success = tryLock(keyName, lockValue, timeout, mode);
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

    /**
     *
     * @param key 锁的名字
     * @param value 锁的内容
     * @param timeout 超时时间
     * @param mode "read" or "write"
     * @return
     */
    public boolean tryLock(String key, String value, Integer timeout, String mode) {
        List<String> list = new ArrayList<>();
        if (mode.equals("read")) {
            String command = "if redis.call('exists', KEYS[2]) == 1 then "
                    + "    return nil; "   // 如果存在写锁，直接加锁失败。
                    + "else "
                    + "    if redis.call('exists', KEYS[1]) == 0 then "                    // 判断指定的key是否存在
                    + "        redis.call('HSET', KEYS[1], ARGV[2], 1) "                    // 不存在新增key，value为hash结构
                    + "        redis.call('PEXPIRE', KEYS[1], ARGV[1]) "                    // 设置过期时间
                    + "    else "
                    + "        if redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1 then "       // key存在说明已经有读锁创建了，接下来判断这个锁是重入锁，还是其他线程的读锁
                    + "            redis.call('HINCRBY', KEYS[1], ARGV[2], 1) "             // hash中指定键的值+1
                    + "            redis.call('PEXPIRE', KEYS[1], ARGV[1]) "                // 重置过期时间
                    + "            return 1; "
                    + "        else "                                                      // 不是重入锁，新锁
                    + "            redis.call('HSET', KEYS[1], ARGV[2], 1) "
                    + "            redis.call('PEXPIRE', KEYS[1], ARGV[1]) "
                    + "        end "
                    + "    end "
                    + "    return 1; "                                                 // 直接返回1，表示加锁成功
                    + "end";
            //list传入 key分为两种 第一种 id 第二种 id-mode。一起创建一起删除

            list.add(command);
            list.add("2"); // keyNum
            list.add("read-" + key); //hash键   KEYS[1]
            list.add("write-" + key); //写锁的key KEYS[2]
            list.add(timeout.toString()); //ARGV[1]
            list.add(value); // 锁的内容 也就是hash的名字 ARGV[2]
            Response result = redisClient.eval(list);
            if (result == null) {
                LOG.debugv("acquire lock fail, keyName:{0}, lockValue:{1}, ttl:{2}", key, value, result.toInteger());
                return false;
            } else {
                LOG.debugv("acquire lock success, keyName:{0}, lockValue:{1}, timeout:{2}", key, value, timeout);
                return true;
            }

        }

        if (mode.equals("write")) {
            String command = "if redis.call('exists', KEYS[2]) == 1 then "
                    + "    return 1; " + // 如果存在读锁，直接加锁失败。
                    "end; " +
                    "if (redis.call('exists', KEYS[1]) == 0) then " +                    //判断指定的key是否存在
                    "    redis.call('hset', KEYS[1], ARGV[2], 1); " +                   //新增key，value为hash结构
                    "    redis.call('pexpire', KEYS[1], ARGV[1]); " +                   //设置过期时间
                    "    return nil; " +                                                //直接返回null，表示加锁成功
                    "end; " +
                    "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +         //判断hash中是否存在指定的建
                    "    redis.call('hincrby', KEYS[1], ARGV[2], 1); " +                //hash中指定键的值+1
                    "    redis.call('pexpire', KEYS[1], ARGV[1]); " +                   //重置过期时间
                    "    return nil; " +                                                //返回null，表示加锁成功
                    "end; " +
                    "return redis.call('pttl', KEYS[1]);";                              //返回key的剩余过期时间，表示加锁失败

            list.add(command);
            list.add("2"); // keyNum
            list.add("write-" + key); //hash键   KEYS[1]
            list.add("read-" + key); //读锁的key KEYS[2]
            list.add(timeout.toString()); //ARGV[1]
            list.add(value); // 锁的内容 也就是hash的名字 ARGV[2]
            Response result = redisClient.eval(list);

            if (result == null) {

                LOG.debugv("acquire lock success, keyName:{0}, lockValue:{1}, timeout:{2}", key, value, timeout);
                return true;
            } else {
                LOG.debugv("acquire lock fail, keyName:{0}, lockValue:{1}, ttl:{2}", key, value, result.toInteger());
                return false;
            }
        }
        LOG.debug("lock mode is incorrect,expected value is \"read\" or \"write\"");
        return false;

    }

    @Override
    public void unlock() {
        releaseLock(keyName, lockValue, timeout, mode);
    }

    /**
     *
     * @param key 锁的名称
     * @param value 锁的内容
     * @param timeout 超时时间
     * @param mode 读写锁类型
     */
    private void releaseLock(String key, String value, Integer timeout, String mode) {
        if (mode.equals("read")) {
            String command = "if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then " +
                    "    return nil; " +                                                       // 判断当前客户端之前是否已获取到锁，若没有直接返回null
                    "end; " +
                    "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " +           // 锁重入次数-1
                    "if (counter > 0) then " +                                                  // 若锁尚未完全释放，需要重置过期时间
                    "    redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    "    return 0; " +                                                          // 返回0表示锁未完全释放
                    "else " +
                    "    redis.call('hdel', KEYS[1], ARGV[2]); " +     //如果hash长度还大于0说明还有读锁在里面
                    "    if redis.call('hlen', KEYS[1]) > 0 then " +
                    "        return 0; " +
                    "    end; " +
                    "    return 1; " +                                                          // 返回1表示锁已完全释放
                    "end; " +
                    "return nil;";
            List<String> list = new ArrayList<>();
            list.add(command);
            list.add("1"); // keyNum
            list.add("read-" + key); //hash键   KEYS[1]
            list.add(timeout.toString()); //ARGV[1]
            list.add(value); // 锁的内容 也就是hash的名字 ARGV[2]
            Response result = redisClient.eval(list);
            if (result == null) {
                LOG.warnv("Current thread does not hold lock, keyName:{0}, lockValue:{1}", key, value);
                throw new RuntimeException("current thread does not hold lock");
            }
            Integer resultNum = result.toInteger();
            if (resultNum == 0) {
                LOG.debugv("release lock sucess, keyName:{0}, lockValue:{1}", key, value);
            } else {
                LOG.debugv("Decrease lock times sucess, keyName:{0}, lockValue:{1}", key, value);
            }
        } else if (mode.equals("write")) {
            String command =
                    "if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then " +
                            "return nil;" +                                                         //判断当前客户端之前是否已获取到锁，若没有直接返回null
                            "end; " +
                            "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " +           //锁重入次数-1
                            "if (counter > 0) then " +                                                  //若锁尚未完全释放，需要重置过期时间
                            "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                            "return 0; " +                                                          //返回0表示锁未完全释放
                            "else " +
                            "redis.call('del', KEYS[1]); " +                                        //若锁已完全释放，删除当前key
                            "return 1; " +                                                          //返回1表示锁已完全释放
                            "end; " +
                            "return nil;";
            List<String> list = new ArrayList<>();
            list.add(command);
            list.add("1");
            list.add("write-" + key);
            list.add(timeout.toString());
            list.add(value);
            Response result = redisClient.eval(list);
            if (result == null) {
                LOG.warnv("Current thread does not hold lock, keyName:{0}, lockValue:{1}", key, value);
                throw new RuntimeException("current thread does not hold lock");
            }
            Integer resultNum = result.toInteger();
            if (resultNum == 1) {
                LOG.debugv("release lock sucess, keyName:{0}, lockValue:{1}", key, value);
            } else {
                LOG.debugv("Decrease lock times sucess, keyName:{0}, lockValue:{1}", key, value);
            }
        }



    }
}
