
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
 * @ClassName RedisSink
 * @Description 自定义Redis Sink
 * @Author xuhaoYF501492
 * @Date 2024/6/22 14:40
 * @Version 1.0
 */
public class EquTypeRedisClusterSink extends RedisSingleNodeSink<String> {


    public EquTypeRedisClusterSink(String redisPassword) {
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
                String tableName = "equ_type";  // 表名为equ_type
                String primaryKey = dataNode.get("type_id").asText();

                // 选择指定字段
                String redisKey = tableName + ":" + primaryKey;
                Map<String, String> hashMap = new HashMap<>();
                hashMap.put("type_id", dataNode.get("type_id").asText());
                hashMap.put("type_code", dataNode.get("type_code").asText());
                hashMap.put("type_name", dataNode.get("type_name").asText());
                hashMap.put("inter_equ", dataNode.get("inter_equ").asText());

                jedis.hmset(redisKey, hashMap);
            } else if ("d".equals(opType)) {  // 处理删除操作
                String tableName = "equ_type";  // 表名为equ_type
                String primaryKey = jsonNode.get("before").get("type_id").asText();

                // 从 Redis 中删除整个哈希
                jedis.del(tableName + ":" + primaryKey);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
