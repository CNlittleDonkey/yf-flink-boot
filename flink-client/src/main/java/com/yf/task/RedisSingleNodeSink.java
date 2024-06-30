package com.yf.task;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Set;

public abstract class RedisSingleNodeSink<T> extends RichSinkFunction<T> {
    public transient Jedis jedis;
    public final String redisPassword;

    public RedisSingleNodeSink(String redisPassword) {
        this.redisPassword = redisPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5);
        config.setMaxWaitMillis(10000);

        String redisHost = "10.10.62.21"; // 单个Redis节点的主机名
        int redisPort = 6379; // 单个Redis节点的端口号

        JedisPool jedisPool = new JedisPool(config, redisHost, redisPort, 10000, redisPassword);

        // 获取Jedis实例
        jedis = jedisPool.getResource();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedis != null) {
            jedis.close();
        }
    }

    protected Jedis getJedis() {
        return jedis;
    }

    @Override
    public abstract void invoke(T value, Context context);


}
