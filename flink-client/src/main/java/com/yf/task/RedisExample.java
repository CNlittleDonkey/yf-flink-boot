package com.yf.task;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.Jedis;
public class RedisExample {
    private String redisHost;
    private int redisPort;
    private String redisPassword;
    private Jedis jedis;

    public RedisExample(String redisHost, int redisPort, String redisPassword) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.jedis = new Jedis(redisHost, redisPort);
        this.jedis.auth(redisPassword);
    }

    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }

    public void storeData(String key, Map<String, String> data) {
        jedis.hmset(key, data);
    }

    public Map<String, String> getData(String key) {
        return jedis.hgetAll(key);
    }

    public static void main(String[] args) {
        RedisExample redisExample = new RedisExample("10.10.62.21", 6379, "redis@hckj");

        String redisKey = "de_energystorage_dimensions_table:1_SN18_CluMaxCellVoltPosi";

        // 示例数据
        BigDecimal coef = new BigDecimal("123.11");

        // 使用 HashMap 存储数据到哈希表
        Map<String, String> data = new HashMap<>();
        data.put("coef", coef.toPlainString());
        redisExample.storeData(redisKey, data);

        // 获取哈希键的所有字段和值
        Map<String, String> redisValue = redisExample.getData(redisKey);
        if (redisValue != null) {
            for (Map.Entry<String, String> entry : redisValue.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
        }

        // 获取哈希键的某个字段的值，并转换为 BigDecimal
        String coefStr = redisValue.get("coef");
        BigDecimal coefValue = new BigDecimal(coefStr);
        System.out.println("Coef value: " + coefValue);

        redisExample.close();
    }
}
