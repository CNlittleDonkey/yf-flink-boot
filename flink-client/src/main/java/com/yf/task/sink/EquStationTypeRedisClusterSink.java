package com.yf.task.sink;

import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.yf.task.RedisSingleNodeSink;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName RedisSink
 * @Description 自定义Redis Sink
 * @Author xuhaoYF501492
 * @Date 2024/6/22 14:40
 * @Version 1.0
 */
public class EquStationTypeRedisClusterSink extends RedisSingleNodeSink<String> {


    public EquStationTypeRedisClusterSink(String redisPassword) {
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
                String tableName = "equ_station_type";  // 表名为equ_station_type
                String primaryKey = dataNode.get("station_type_id").asText();

                // 选择相关字段
                String redisKey = tableName + ":" + primaryKey;
                Map<String, String> hashMap = new HashMap<>();
                hashMap.put("station_type_id", dataNode.get("station_type_id").asText());
                hashMap.put("station_type_code", dataNode.get("station_type_code").asText());
                hashMap.put("inter_station", dataNode.get("inter_station").asText());

                jedis.hmset(redisKey, hashMap);
            } else if ("d".equals(opType)) {  // 处理删除操作
                String tableName = "equ_station_type";  // 表名为equ_station_type
                String primaryKey = jsonNode.get("before").get("station_type_id").asText();

                // 从 Redis 中删除整个哈希
                jedis.del(tableName + ":" + primaryKey);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
