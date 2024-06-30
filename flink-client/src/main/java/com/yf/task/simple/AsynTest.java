package com.yf.task.simple;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.HostAndPort;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.JedisCluster;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.JedisPool;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.JedisPoolConfig;
import com.yf.task.pojo.EnrichedStatMutation;
import com.yf.task.pojo.StatMutation;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.configuration.Configuration;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class AsynTest extends RichAsyncFunction<StatMutation, List<EnrichedStatMutation>> {

    private transient JedisCluster jedisCluster;
    private Map<String, JedisPool> clusterNodes;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5);
        config.setMaxWaitMillis(10000);

        Set<HostAndPort> redisNodes = new HashSet<>();
        redisNodes.add(new HostAndPort("10.10.5.154", 6379));
        redisNodes.add(new HostAndPort("10.10.5.153", 6379));
        redisNodes.add(new HostAndPort("10.10.5.152", 6379));

        String redisPassword = "foreverLuky99"; // Redis密码

        jedisCluster = new JedisCluster(redisNodes, 1000, 1000, 5, redisPassword, config);
        clusterNodes = jedisCluster.getClusterNodes();
        System.out.println("JedisCluster initialized: " + jedisCluster);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedisCluster != null) {
            jedisCluster.close();
        }
    }


    @Override
    public void asyncInvoke(StatMutation input, ResultFuture<List<EnrichedStatMutation>> resultFuture) throws Exception {
        // 异步执行 Redis 查询，并将结果传递给 ResultFuture
        CompletableFuture.runAsync(() -> {
            try {
                List<Map<String, String>> aggrStationRelateData = fetchAggrStationRelateData(input.getStation());
                // 假设你需要将数据转换成 EnrichedStatMutation 对象
                List<EnrichedStatMutation> enrichedStatMutations = new ArrayList<>();
                for (Map<String, String> data : aggrStationRelateData) {
                    EnrichedStatMutation enriched = new EnrichedStatMutation();
                    enriched.setCabinetNo(data.get("cabinet_no"));
                    // 根据需要设置其他字段
                    enrichedStatMutations.add(enriched);
                }
                resultFuture.complete(Collections.singleton(enrichedStatMutations));
            } catch (Exception e) {
                resultFuture.completeExceptionally(e);
            }
        });
    }


    private List<Map<String, String>> fetchAggrStationRelateData(String stationId) {
        List<Map<String, String>> result = new ArrayList<>();

        if (clusterNodes == null || clusterNodes.isEmpty()) {
            System.err.println("Cluster nodes are empty or null");
            return result;
        }

        System.out.println("Cluster nodes: " + clusterNodes.keySet());

        for (String nodeKey : clusterNodes.keySet()) {
            JedisPool jedisPool = clusterNodes.get(nodeKey);
            try (Jedis jedis = jedisPool.getResource()) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match("equ_aggr_station_relate:*").count(100); // Count指定每次扫描的数量
                do {
                    ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                    cursor = scanResult.getStringCursor();
                    for (String key : scanResult.getResult()) {
                        Map<String, String> hash = jedisCluster.hgetAll(key);
                        // 根据 stationId 进行过滤
                        if (stationId.equals(hash.get("station_id"))) {
                            result.add(hash);
                        }
                    }
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            } catch (Exception e) {
                System.err.println("Error accessing node " + nodeKey + ": " + e.getMessage());
            }
        }

        return result;
    }
}
