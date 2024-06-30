package com.yf.task.simple;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.*;
import com.yf.task.pojo.EnergyStorageDimension;

import java.math.BigDecimal;
import java.util.*;

import static com.yf.until.ContainFun.*;

public class RedisDataFetcher {

    private  transient  JedisCluster jedisCluster;

    public RedisDataFetcher() {
    }


}