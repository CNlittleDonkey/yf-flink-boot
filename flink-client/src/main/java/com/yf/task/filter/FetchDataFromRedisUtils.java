package com.yf.task.filter;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.JedisCluster;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.ScanParams;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.ScanResult;
import com.yf.task.pojo.PackedLogicEquAndParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName FetchDataFromRedisUtils
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/28 8:52
 * @Version 1.0
 */
public class FetchDataFromRedisUtils {

    public static  PackedLogicEquAndParam equ_Logic_Equ_FetchedData(String stationId, String emuSn, String cabinet_no, String paramSn , JedisCluster jedisCluster) {
        int cursor = 0;
        Map<String, Map<String, String>> filteredData = new HashMap<>();
        do {
            ScanResult<String> scanResult = jedisCluster.scan(String.valueOf(cursor), new ScanParams().match("equ_logic_equ*"));
            cursor = scanResult.getCursor();
            for (String key : scanResult.getResult()) {
                Map<String, String> hash = jedisCluster.hgetAll(key);
                if (stationId.equals(hash.get("station_id")) && emuSn.equals(hash.get("emu_sn"))&& cabinet_no.equals(hash.get("cabinet_no"))) {
                    // 处理符合条件的数据
                    filteredData.put(key, new HashMap<>(hash)); // 添加符合条件的键和值到Map中
                    System.out.println("Matched key: " + key + ", data: " + hash);
                }
            }
        } while (cursor!=0);


        Map<String, Map<String, String>> result = new HashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : filteredData.entrySet()) {
            String logicEquId = entry.getValue().get("logic_equ_id");
            if (logicEquId != null) {
                Map<String, String> paramData = fetchParamData(logicEquId, paramSn,jedisCluster);
                if (paramData != null) {
                    result.put("equ_le_param:" + logicEquId, paramData);
                }
            }
        }
        return new PackedLogicEquAndParam(filteredData,result);
    }

    private static Map<String, String> fetchParamData(String logicEquId, String paramSn, JedisCluster jedisCluster) {
        String redisKey = "equ_le_param:" + logicEquId;
        Map<String, String> paramData = jedisCluster.hgetAll(redisKey);
        if (paramData.isEmpty() || !paramSn.equals(paramData.get("param_sn"))) {
            return null;
        }
        return paramData;
    }

    public static Map<String, Map<String, String>> fetchEquStationData(String stationId, JedisCluster jedisCluster) {
        Map<String, Map<String, String>> stationDataMap = new HashMap<>();
            String redisKey = "equ_station:" + stationId;
            Map<String, String> stationData = jedisCluster.hgetAll(redisKey);
            if (stationData != null && !stationData.isEmpty()) {
                stationDataMap.put(redisKey, stationData);
            }
        return stationDataMap;
    }
}
