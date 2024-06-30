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
public class EquLeParamRedisClusterSink extends RedisSingleNodeSink<String> {


    public EquLeParamRedisClusterSink(String redisPassword) {
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
            if (recovery && ("c".equals(opType) || "u".equals(opType)) ||  "r".equals(opType) ) {  // 处理插入和更新操作
                String tableName = "equ_le_param";  // 表名为equ_le_param
                String primaryKey = dataNode.get("param_id").asText();

                // 选择指定字段
                String redisKey = tableName + ":" + primaryKey;
                Map<String, String> hashMap = new HashMap<>();
                hashMap.put("param_id", dataNode.get("param_id").asText());
                hashMap.put("param_code", dataNode.get("param_code").asText());
                hashMap.put("param_name", dataNode.get("param_name").asText());
                hashMap.put("param_type", dataNode.get("param_type").asText());
                hashMap.put("param_claz", dataNode.get("param_claz").asText());
                hashMap.put("station_id", dataNode.get("station_id").asText());
                hashMap.put("type_id", dataNode.get("type_id").asText());
                hashMap.put("logic_equ_id", dataNode.get("logic_equ_id").asText());
                hashMap.put("param_unit", dataNode.get("param_unit").asText());
                hashMap.put("range_upper", dataNode.get("range_upper").asText());
                hashMap.put("range_lower", dataNode.get("range_lower").asText());
                hashMap.put("invalid_value", dataNode.get("invalid_value").asText());
                hashMap.put("alm_claz", dataNode.get("alm_claz").asText());
                hashMap.put("alm_level", dataNode.get("alm_level").asText());
                hashMap.put("main_advise", dataNode.get("main_advise").asText());
                hashMap.put("recovery", dataNode.get("recovery").asText());
                hashMap.put("status", dataNode.get("status").asText());
                hashMap.put("msg_rule_id", dataNode.get("msg_rule_id").asText());
                hashMap.put("script", dataNode.get("script").asText());
                hashMap.put("relate_param_code", dataNode.get("relate_param_code").asText());
                hashMap.put("cust_view", dataNode.get("cust_view").asText());
                hashMap.put("cust_alm_name", dataNode.get("cust_alm_name").asText());

                // 处理coef字段
                JsonNode coefNode = dataNode.get("coef");
                String coef = (coefNode == null ||  coefNode.asText().isEmpty()) ? "1" : coefNode.asText();
                hashMap.put("coef", coef);
                jedis.hmset(redisKey, hashMap);
            } else if ("d".equals(opType)) {  // 处理删除操作
                String tableName = "equ_le_param";  // 表名为equ_le_param
                String primaryKey = jsonNode.get("before").get("param_id").asText();

                // 从 Redis 中删除整个哈希
                jedis.del(tableName + ":" + primaryKey);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
