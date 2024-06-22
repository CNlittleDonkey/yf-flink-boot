package com.yf.task.simple;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.Jedis;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsyncRedisLookupFunction extends RichAsyncFunction<StatMutation, EnrichedStatMutation> {

    private final String redisHost;
    private final int redisPort;
    private transient Jedis jedis;
    private final ExecutorService executorService;
    private transient Cache<String, EnergyStorageDimension> cache;

    public AsyncRedisLookupFunction(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.executorService = Executors.newFixedThreadPool(20);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.jedis = new Jedis(redisHost, redisPort);
        this.cache = CacheBuilder.newBuilder()
                .expireAfterWrite(60, TimeUnit.SECONDS)  // 设置缓存过期时间
                .maximumSize(100000)
                .build();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedis != null) {
            jedis.close();
        }
        executorService.shutdown();
    }

    @Override
    public void asyncInvoke(StatMutation input, ResultFuture<EnrichedStatMutation> resultFuture) {
        CompletableFuture.supplyAsync(() -> {
            try {
                String cacheKey = "ibt_energystorage_dimensions_view" + "_" + input.getCabinet() + "_" + input.getEmuSn() + "_" + input.getName();
                EnergyStorageDimension cachedDim = cache.getIfPresent(cacheKey);
                if (cachedDim != null) {
                    EnrichedStatMutation enrichedStatMutation = createEnrichedStatMutation(input, cachedDim);
                    return enrichedStatMutation;
                } else {
                    String redisKey = "ibt_energystorage_dimensions_view";
                    String redisField = input.getCabinet() + "_" + input.getEmuSn() + "_" + input.getName();
                    String redisValue = jedis.hget(redisKey, redisField);
                    if (redisValue != null) {
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

    private EnrichedStatMutation createEnrichedStatMutation(StatMutation input, EnergyStorageDimension dim) {
        return new EnrichedStatMutation(
                dim.getMeasuringId(),
                dim.getAggrStationId(),
                dim.getAggrStationCode(),
                dim.getAggrStationName(),
                dim.getStationId(),
                dim.getStationCode(),
                dim.getStationName(),
                dim.getStationAbbr(),
                dim.getProdSeries(),
                dim.getCabinetNo(),
                dim.getEmuSn(),
                dim.getStaCapacity(),
                dim.getTypeId(),
                dim.getTypeCode(),
                dim.getTypeName(),
                dim.getLogicEquId(),
                dim.getLogicEquCode(),
                dim.getLogicEquName(),
                dim.getInterEqu(),
                dim.getParamId(),
                dim.getParamCode(),
                dim.getParamType(),
                dim.getParamName(),
                dim.getParamClaz(),
                dim.getCoef(),
                dim.getAlmClaz(),
                dim.getAlmLevel(),
                dim.getNoAlm(),
                dim.getFaultMonitor(),
                dim.getMainAdvise(),
                dim.getRangeUpper(),
                dim.getRangeLower(),
                dim.getInvalidValue(),
                dim.getExpValue(),
                dim.getRecovery(),
                dim.getStatus(),
                dim.getParamSn(),
                input.getRaw(),
                input.getRaw().multiply(dim.getCoef()),
                calculateMeasNo(input.getTime()),
                input.getTime(),
                dim.getTenantId(),
                dim.getMsgRuleId(),
                dim.getScript(),
                dim.getRelateParamCode(),
                dim.getCustView(),
                dim.getCustAlmName(),
                dim.getTestAlm()
        );
    }

    private EnergyStorageDimension parseRedisValue(String redisValue) {
        // 解析Redis中的值，并转换为EnergyStorageDimension对象
        String[] fields = redisValue.split(",");
        EnergyStorageDimension dim = new EnergyStorageDimension();
        dim.setMeasuringId(fields[0]);
        dim.setAggrStationId(Long.parseLong(fields[1]));
        dim.setAggrStationCode(fields[2]);
        dim.setAggrStationName(fields[3]);
        dim.setStationId(Long.parseLong(fields[4]));
        dim.setStationCode(fields[5]);
        dim.setStationName(fields[6]);
        dim.setStationAbbr(fields[7]);
        dim.setProdSeries(fields[8]);
        dim.setStaCapacity(new BigDecimal(fields[9]));
        dim.setTypeId(Long.parseLong(fields[10]));
        dim.setTypeCode(fields[11]);
        dim.setTypeName(fields[12]);
        dim.setLogicEquId(Long.parseLong(fields[13]));
        dim.setLogicEquCode(fields[14]);
        dim.setLogicEquName(fields[15]);
        dim.setInterEqu(fields[16]);
        dim.setParamId(Long.parseLong(fields[17]));
        dim.setParamCode(fields[18]);
        dim.setParamType(fields[19]);
        dim.setParamName(fields[20]);
        dim.setParamClaz(fields[21]);
        dim.setCoef(new BigDecimal(fields[22]));
        dim.setAlmClaz(fields[23]);
        dim.setAlmLevel(fields[24]);
        dim.setNoAlm(Boolean.parseBoolean(fields[25]));
        dim.setFaultMonitor(Boolean.parseBoolean(fields[26]));
        dim.setMainAdvise(fields[27]);
        dim.setRangeUpper(new BigDecimal(fields[28]));
        dim.setRangeLower(new BigDecimal(fields[29]));
        dim.setInvalidValue(fields[30]);
        dim.setExpValue(fields[31]);
        dim.setRecovery(Boolean.parseBoolean(fields[32]));
        dim.setStatus(fields[33]);
        dim.setTenantId(Long.parseLong(fields[34]));
        dim.setMsgRuleId(Long.parseLong(fields[35]));
        dim.setScript(fields[36]);
        dim.setRelateParamCode(fields[37]);
        dim.setCustView(Boolean.parseBoolean(fields[38]));
        dim.setCustAlmName(fields[39]);
        dim.setTestAlm(Boolean.parseBoolean(fields[40]));
        return dim;
    }

    private long calculateMeasNo(long time) {
        return Timestamp.valueOf("1970-01-01 00:00:00").toLocalDateTime()
                .until(Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime(), ChronoUnit.HOURS);
    }
}
