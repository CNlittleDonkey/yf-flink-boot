package com.yf.task.simple;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.HostAndPort;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.JedisCluster;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.JedisPoolConfig;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.yf.task.pojo.EnergyStorageDimension;
import com.yf.task.pojo.EnrichedStatMutation;
import com.yf.task.pojo.StatMutation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.yf.task.filter.ParseValueUtil.createEnrichedStatMutation;

public class AsyncRedisLookupFunction extends RichAsyncFunction<StatMutation, EnrichedStatMutation> {

    private final String redisPassword;
    private transient JedisCluster jedisCluster;
    private transient ExecutorService executorService;
    private transient Cache<String, EnergyStorageDimension> cache;

    public AsyncRedisLookupFunction( String redisPassword) {

        this.redisPassword = redisPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5);
        config.setMaxWaitMillis(10000);
        Set<HostAndPort> redisNodes = new HashSet<>();
        redisNodes.add(new HostAndPort("10.10.5.154", 6379));
        redisNodes.add(new HostAndPort("10.10.5.153", 6379));
        redisNodes.add(new HostAndPort("10.10.5.152", 6379));
        jedisCluster = new JedisCluster(redisNodes,1000,1000,5,redisPassword,config);

        this.cache = CacheBuilder.newBuilder()
                .expireAfterWrite(1, TimeUnit.SECONDS)  // 设置缓存过期时间
                .maximumSize(100000)
                .build();
        this.executorService = Executors.newFixedThreadPool(1);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedisCluster != null) {
            jedisCluster.close();
        }
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Override
    public void asyncInvoke(StatMutation input, ResultFuture<EnrichedStatMutation> resultFuture) {
        CompletableFuture.supplyAsync(() -> {
            try {
                String cacheKey = "de_energystorage_dimensions_table" + ":" + input.getCabinet_no() + "_" + input.getEmu_sn() + "_" + input.getName();
                // System.out.println(cacheKey);
                EnergyStorageDimension cachedDim = cache.getIfPresent(cacheKey);
                if (cachedDim != null) {
                    EnrichedStatMutation enrichedStatMutation = createEnrichedStatMutation(input, cachedDim);
                    return enrichedStatMutation;
                } else {
                    String keyType = jedisCluster.type(cacheKey);
                    // System.out.println("Key type: " + keyType);
                    Map<String, String> redisValue = jedisCluster.hgetAll(cacheKey);

                    if (redisValue != null && !redisValue.isEmpty()) {
                        EnergyStorageDimension dim = parseRedisValue(redisValue);
                        cache.put(cacheKey, dim);
                        EnrichedStatMutation enrichedStatMutation = createEnrichedStatMutation(input, dim);
                        return enrichedStatMutation;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }, executorService).thenAccept(result -> {
            if (result != null) {
                resultFuture.complete(Collections.singleton(result));
            } else {
                resultFuture.complete(Collections.emptyList());
            }
        });
    }



    public static EnergyStorageDimension parseRedisValue(Map<String, String> redisValue) {
        EnergyStorageDimension dim = new EnergyStorageDimension();

        dim.setMeasuringId(redisValue.get("measuringId"));
        dim.setAggrStationId(parseLong(redisValue.get("aggrStationId")));
        dim.setAggrStationCode(redisValue.get("aggrStationCode"));
        dim.setAggrStationName(redisValue.get("aggrStationName"));
        dim.setStationId(parseLong(redisValue.get("stationId")));
        dim.setStationCode(redisValue.get("stationCode"));
        dim.setStationName(redisValue.get("stationName"));
        dim.setStationAbbr(redisValue.get("stationAbbr"));
        dim.setStationTypeId(parseLong(redisValue.get("stationTypeId")));
        dim.setStationTypeCode(redisValue.get("stationTypeCode"));
        dim.setInterStation(redisValue.get("interStation"));
        dim.setStaCapacity(parseBigDecimal(redisValue.get("staCapacity")));
        dim.setTypeId(parseLong(redisValue.get("typeId")));
        dim.setTypeCode(redisValue.get("typeCode"));
        dim.setTypeName(redisValue.get("typeName"));
        dim.setLogicEquId(parseLong(redisValue.get("logicEquId")));
        dim.setLogicEquCode(redisValue.get("logicEquCode"));
        dim.setLogicEquName(redisValue.get("logicEquName"));
        dim.setIndicatorTempId(parseLong(redisValue.get("indicatorTempId")));
        dim.setModel(redisValue.get("model"));
        dim.setInterEqu(redisValue.get("interEqu"));
        dim.setParamId(parseLong(redisValue.get("paramId")));
        dim.setParamCode(redisValue.get("paramCode"));
        dim.setParamType(redisValue.get("paramType"));
        dim.setParamName(redisValue.get("paramName"));
        dim.setParamClaz(redisValue.get("paramClaz"));
        dim.setCoef(parseBigDecimal(redisValue.get("coef")));
        dim.setAlmClaz(redisValue.get("almClaz"));
        dim.setAlmLevel(redisValue.get("almLevel"));
        dim.setNoAlm(parseBoolean(redisValue.get("noAlm")));
        dim.setFaultMonitor(parseBoolean(redisValue.get("faultMonitor")));
        dim.setMainAdvise(redisValue.get("mainAdvise"));
        dim.setRangeUpper(parseBigDecimal(redisValue.get("rangeUpper")));
        dim.setRangeLower(parseBigDecimal(redisValue.get("rangeLower")));
        dim.setInvalidValue(redisValue.get("invalidValue"));
        dim.setExpValue(redisValue.get("expValue"));
        dim.setRecovery(parseBoolean(redisValue.get("recovery")));
        dim.setStatus(redisValue.get("status"));
        dim.setEmuSn(redisValue.get("emuSn"));
        dim.setCabinetNo(redisValue.get("cabinetNo"));
        dim.setParamSn(redisValue.get("paramSn"));
        dim.setTenantId(parseLong(redisValue.get("tenantId")));
        dim.setMsgRuleId(parseLong(redisValue.get("msgRuleId")));
        dim.setScript(redisValue.get("script"));
        dim.setRelateParamCode(redisValue.get("relateParamCode"));
        dim.setCustView(parseBoolean(redisValue.get("custView")));
        dim.setCustAlmName(redisValue.get("custAlmName"));
        return dim;
    }

    private static long parseLong(String value) {
        return value == null ? 0L : Long.parseLong(value);
    }

    private static BigDecimal parseBigDecimal(String value) {
        if (value == null || value.isEmpty()) {
            return BigDecimal.ZERO;
        }
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            System.out.println("-----------" + value);
            e.printStackTrace(); // 打印堆栈信息以便调试
            return BigDecimal.ZERO; // 或者根据你的业务逻辑选择其他默认值
        }
    }

    private static boolean parseBoolean(String value) {
        return value != null && Boolean.parseBoolean(value);
    }

    private long calculateMeasNo(long time) {
        return Timestamp.valueOf("1970-01-01 00:00:00").toLocalDateTime()
                .until(Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime(), ChronoUnit.HOURS);
    }
}
