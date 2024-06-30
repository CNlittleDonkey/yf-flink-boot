package com.yf.task;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.*;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.exceptions.JedisMovedDataException;

import java.util.*;

public class RedisClusterTest {

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

        jedisCluster = new JedisCluster(redisNodes, 1000, 1000, 5, redisPassword, config);
    }

    public List<Map.Entry<String, Map<String, String>>> filterEntries() {
        List<Map.Entry<String, Map<String, String>>> filteredEntries = new ArrayList<>();
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();

        for (Map.Entry<String, JedisPool> entry : clusterNodes.entrySet()) {
            String nodeAddress = entry.getKey();
            JedisPool jedisPool = entry.getValue();
            try (Jedis jedis = jedisPool.getResource()) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match("equ_station_attr:*").count(100); // Count指定每次扫描的数量
                do {
                    try {
                        ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                        cursor = scanResult.getStringCursor();
                        for (String key : scanResult.getResult()) {
                            Map<String, String> hash = jedis.hgetAll(key);
                            // 根据 stationId 和 attrCode 进行过滤
                            if ("720".equals(hash.get("station_id"))) {
                                filteredEntries.add(new AbstractMap.SimpleEntry<>(key, hash));
                                if ("StaCapacity".equals(hash.get("attr_code"))) {
                                    System.out.println("StaCapacity attr found for key: " + key);
                                }
                            }
                        }
                    } catch (JedisMovedDataException e) {
                        System.err.println("MOVED data error: " + e.getMessage() + ". Retrying with new node.");
                        cursor = ScanParams.SCAN_POINTER_START; // reset cursor
                        // The moved data exception will be handled by JedisCluster automatically, no need to retry manually.
                    }
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            } catch (Exception e) {
                System.err.println("Error accessing node " + nodeAddress + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        return filteredEntries;
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

    public static void main(String[] args) {
        RedisClusterTest example = new RedisClusterTest();
        example.openConnection();

        List<Map.Entry<String, Map<String, String>>> filteredEntries = example.filterEntries();
        System.out.println("Filtered entries:");
        for (Map.Entry<String, Map<String, String>> entry : filteredEntries) {
            System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
        }

        example.closeConnection();
    }
}
