package com.yf.task.pojo;

import java.util.Map;

/**
 * @ClassName PackedLogicEquAndParam
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/28 9:43
 * @Version 1.0
 */
public class PackedLogicEquAndParam {
    Map<String, Map<String, String>>  EquLogicEquFromRedis;
    Map<String, Map<String, String>>  EquLeParamFromRedis;


    public PackedLogicEquAndParam(Map<String, Map<String, String>> equLogicEquFromRedis, Map<String, Map<String, String>> equLeParamFromRedis) {
        EquLogicEquFromRedis = equLogicEquFromRedis;
        EquLeParamFromRedis = equLeParamFromRedis;
    }

    public Map<String, Map<String, String>> getEquLogicEquFromRedis() {
        return EquLogicEquFromRedis;
    }

    public void setEquLogicEquFromRedis(Map<String, Map<String, String>> equLogicEquFromRedis) {
        EquLogicEquFromRedis = equLogicEquFromRedis;
    }

    public Map<String, Map<String, String>> getEquLeParamFromRedis() {
        return EquLeParamFromRedis;
    }

    public void setEquLeParamFromRedis(Map<String, Map<String, String>> equLeParamFromRedis) {
        EquLeParamFromRedis = equLeParamFromRedis;
    }
}
