package com.yf.task;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.*;

import java.util.*;

public class RedisClusterTest1 {

    private JedisCluster jedisCluster;

    public void openConnection() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5);
        config.setMaxWaitMillis(10000);

        Set<HostAndPort> redisNodes = new HashSet<>();
        redisNodes.add(new HostAndPort("10.10.5.154", 6379));
        redisNodes.add(new HostAndPort("10.10.5.153", 6379));
        redisNodes.add(new HostAndPort("10.10.5.152", 6379));

        String redisPassword = "foreverLuky99"; // Redis密码，如果没有密码可以为null或者空字符串

        try {
            jedisCluster = new JedisCluster(redisNodes, 1000, 1000, 5, redisPassword, config);
            System.out.println("JedisCluster initialized successfully.");
        } catch (Exception e) {
            System.err.println("Error initializing JedisCluster: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void flushAllData() {
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        for (Map.Entry<String, JedisPool> entry : clusterNodes.entrySet()) {
            String nodeAddress = entry.getKey();
            JedisPool jedisPool = entry.getValue();
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.flushAll();
                System.out.println("Data flushed on node: " + nodeAddress);
            } catch (Exception e) {
                System.err.println("Error flushing data on node: " + nodeAddress);
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        RedisClusterTest1 example = new RedisClusterTest1();
        example.openConnection();

        // Flush all data
        example.flushAllData();

        example.closeConnection();
    }

    public void closeConnection() {
        if (jedisCluster != null) {
            try {
                jedisCluster.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
