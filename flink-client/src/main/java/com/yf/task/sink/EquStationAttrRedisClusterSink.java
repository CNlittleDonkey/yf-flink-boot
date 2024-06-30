package com.yf.task.sink;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.HostAndPort;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.JedisCluster;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.yf.task.RedisSingleNodeSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * @ClassName RedisSink
 * @Description 自定义Redis Sink
 * @Author xuhaoYF501492
 * @Date 2024/6/22 14:40
 * @Version 1.0
 */
public class EquStationAttrRedisClusterSink extends RedisSingleNodeSink<String> {

    public EquStationAttrRedisClusterSink(String redisPassword) {
        super(redisPassword);
    }

    @Override
    public void invoke(String value, Context context) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(value);

            String opType = jsonNode.get("op").asText();
            JsonNode dataNode = jsonNode.get("after");  // CDC事件中的新数据

            // 检查recovery字段并确保attr_code为'StaCapacity'
            boolean recovery = dataNode.get("recovery").asInt() == 0;
            boolean isStaCapacity = "StaCapacity".equals(dataNode.get("attr_code").asText());

            if (recovery && isStaCapacity && ("c".equals(opType) || "u".equals(opType)) ||  "r".equals(opType)) {  // 处理插入和更新操作
                String tableName = "equ_station_attr";  // 表名为equ_station_attr
                //equ_station_attr  表中的主键是 PRIMARY KEY (`attr_id`)
                String primaryKey = dataNode.get("attr_id").asText();

                // 选择指定字段，并处理attr_val逻辑
                String redisKey = tableName + ":" + primaryKey;
                Map<String, String> hashMap = new HashMap<>();
                hashMap.put("station_id", dataNode.get("station_id").asText());
                hashMap.put("attr_code", dataNode.get("attr_code").asText());
                hashMap.put("sta_capacity", processAttrVal(dataNode.get("attr_val")));

                jedis.hmset(redisKey, hashMap);
            } else if ("d".equals(opType)) {  // 处理删除操作
                String tableName = "equ_station_attr";  // 表名为equ_station_attr
                String primaryKey = jsonNode.get("before").get("station_id").asText();

                // 从 Redis 中删除整个哈希
                jedis.del(tableName + ":" + primaryKey);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String processAttrVal(JsonNode attrValNode) {
        if (attrValNode == null || attrValNode.isEmpty()) {
            return BigDecimal.ZERO.setScale(4).toPlainString();
        } else {
            try {
                return new BigDecimal(attrValNode.asText()).setScale(4).toPlainString();
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return BigDecimal.ZERO.setScale(4).toPlainString();
            }
        }
    }
}
