package com.yf.task.sink;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.HostAndPort;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.JedisCluster;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.JedisPoolConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashSet;
import java.util.Set;

/**
 * @ClassName RedisClusterSink
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/27 11:19
 * @Version 1.0
 */
public abstract class RedisClusterSink<T> extends RichSinkFunction<T> {
    public transient JedisCluster jedisCluster;
    public final String redisPassword;



    public RedisClusterSink(String redisPassword) {
        this.redisPassword = redisPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5);
        config.setMaxWaitMillis(10000);
        Set<HostAndPort> redisNodes = new HashSet<>();
        redisNodes.add(new HostAndPort("10.10.62.21", 6379));
      /*  redisNodes.add(new HostAndPort("10.10.5.153", 6379));
        redisNodes.add(new HostAndPort("10.10.5.152", 6379));*/
        jedisCluster = new JedisCluster(redisNodes,1000,1000,5,redisPassword,config);

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedisCluster != null) {
            jedisCluster.close();
        }
    }
    protected JedisCluster getJedisCluster() {
        return jedisCluster;
    }

    public abstract void invoke(String value, Context context);
}
