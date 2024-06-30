package com.yf.task;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.*;


import java.util.*;

public class RedisClusterExample {

    private JedisCluster jedisCluster;

    public void openConnection() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5);
        config.setMaxWaitMillis(10000);

        Set<HostAndPort> redisNodes = new HashSet<>();
        redisNodes.add(new HostAndPort("10.10.5.154", 6379));
        redisNodes.add(new HostAndPort("10.10.5.153", 6379));
        redisNodes.add(new HostAndPort("10.10.5.152", 6379));

        String redisPassword = "foreverLuky99";
        jedisCluster = new JedisCluster(redisNodes, 1000, 1000, 5, redisPassword, config);
    }

    public List<Map.Entry<String, Map<String, String>>> fetchEquLogicEquData(String stationId, String emuSn, String cabinetNo) {
        List<Map.Entry<String, Map<String, String>>> filteredEntries = new ArrayList<>();
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();

        for (String nodeKey : clusterNodes.keySet()) {
            JedisPool jedisPool = clusterNodes.get(nodeKey);
            try (Jedis jedis = jedisPool.getResource()) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match("equ_le_param:*").count(100); // Count指定每次扫描的数量
                do {
                    ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                    cursor = scanResult.getStringCursor();
                    for (String key : scanResult.getResult()) {
                        Map<String, String> hash = jedisCluster.hgetAll(key);
                        // 根据 stationId、emuSn 和 cabinetNo 进行过滤
                        if ("运行状态".equals(hash.get("param_name"))&&"140".equals(hash.get("logic_equ_id"))) {
                            filteredEntries.add(new AbstractMap.SimpleEntry<>(key, hash));
                        }
                    }
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
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
        RedisClusterExample example = new RedisClusterExample();
        example.openConnection();

        String stationId = "140";
        String emuSn = "SN14";
        String cabinetNo = "1";

        List<Map.Entry<String, Map<String, String>>> filteredEntries = example.fetchEquLogicEquData(stationId, emuSn, cabinetNo);
        System.out.println("Filtered entries:");
        for (Map.Entry<String, Map<String, String>> entry : filteredEntries) {
            System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
        }

        example.closeConnection();
    }
}
