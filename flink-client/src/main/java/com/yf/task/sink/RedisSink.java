package com.yf.task.sink;


import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.Jedis;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.JedisPool;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.JedisPoolConfig;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName RedisSink
 * @Description 自定义Redis Sink
 * @Author xuhaoYF501492
 * @Date 2024/6/22 14:40
 * @Version 1.0
 */
public class RedisSink extends RichSinkFunction<String> {
    private transient JedisPool jedisPool;
    private final String redisHost;
    private final int redisPort;
    private final String redisPassword;

    public RedisSink(String redisHost, int redisPort, String redisPassword) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Override
    public void invoke(String value, Context context) {
        try (Jedis jedis = jedisPool.getResource()) {
            // 进行身份验证
            if (redisPassword != null && !redisPassword.isEmpty()) {
                jedis.auth(redisPassword);
            }

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(value);

            String opType = jsonNode.get("op").asText();
            JsonNode dataNode = jsonNode.get("after");  // CDC事件中的新数据

            if ("c".equals(opType) || "u".equals(opType)) {  // 处理插入和更新操作
                String baseKey = "de_energystorage_dimensions_table";
                String uniqueKey = dataNode.get("cabinet_no").asText() + "_" + dataNode.get("emu_sn").asText() + "_" + dataNode.get("param_sn").asText();

                // 遍历所有字段并将其保存到 Redis 的哈希中
                String redisKey = baseKey + ":" + uniqueKey;
                Map<String, String> hashMap = new HashMap<>();
                dataNode.fields().forEachRemaining(entry -> hashMap.put(entry.getKey(), entry.getValue().asText()));
                // 将字段和值保存到 Redis 哈希
               // hashMap.forEach((field, fieldValue) -> jedis.hset(redisKey, field, fieldValue));
                jedis.hmset(redisKey, hashMap);
                System.out.println(uniqueKey);
            } else if ("d".equals(opType)) {  // 处理删除操作
                String baseKey = "de_energystorage_dimensions_table";
                String uniqueKey = jsonNode.get("before").get("cabinet_no").asText() + "_" + jsonNode.get("before").get("emu_sn").asText() + "_" + jsonNode.get("before").get("param_sn").asText();

                // 从 Redis 中删除整个哈希
                jedis.del(baseKey + ":" + uniqueKey);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
