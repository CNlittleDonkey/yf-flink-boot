package com.yf.task.sink;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.HostAndPort;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.JedisCluster;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.yf.task.RedisSingleNodeSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * @ClassName AggrStationRelateRedisClusterSink
 * @Description 自定义Redis Sink
 * @Author xuhaoYF501492
 * @Date 2024/6/22 14:40
 * @Version 1.0
 */
public class EquAggrStationRelateRedisClusterSink extends RedisSingleNodeSink<String> {


    public EquAggrStationRelateRedisClusterSink(String redisPassword) {
        super(redisPassword);
    }

    @Override
    public void invoke(String value, Context context) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(value);

            String opType = jsonNode.get("op").asText();
            JsonNode dataNode = jsonNode.get("after");  // CDC事件中的新数据

            // 检查recovery字段
            boolean recovery = dataNode.get("recovery").asInt() == 0;
            if (recovery && ("c".equals(opType) || "u".equals(opType)) ||  "r".equals(opType)) {  // 处理插入和更新操作
                String tableName = "equ_aggr_station_relate";  // 假设表名为equ_aggr_station_relate
               //TO DO表中station_relate是主键
                String primaryKey = dataNode.get("station_relate").asText();

                // 只选择station_id, aggr_station_id字段
                String redisKey = tableName + ":" + primaryKey;
                Map<String, String> hashMap = new HashMap<>();
                hashMap.put("station_id", dataNode.get("station_id").asText());
                hashMap.put("aggr_station_id", dataNode.get("aggr_station_id").asText());

                jedis.hmset(redisKey, hashMap);
            } else if ("d".equals(opType)) {  // 处理删除操作
                String tableName = "equ_aggr_station_relate";  // 假设表名为equ_aggr_station_relate
                String primaryKey = jsonNode.get("before").get("station_relate").asText();

                // 从 Redis 中删除整个哈希
                jedis.del(tableName + ":" + primaryKey);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
