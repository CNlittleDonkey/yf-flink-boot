package com.yf.task.simple;

import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.JsonNode;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.yf.task.filter.ParseValueUtil;
import com.yf.task.pojo.EnergyStorageDimension;
import com.yf.task.pojo.EnrichedStatMutation;
import com.yf.task.pojo.StatMutation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.ibm.icu.text.PluralRules.Operand.e;
import static com.yf.until.ContainFun.*;

public class AsyncRedisLookupFunctionSingleNode extends RichAsyncFunction<StatMutation, List<EnrichedStatMutation>> {

    private final String redisPassword;
    private  Jedis jedis;
    private transient ExecutorService executorService;
    private transient Cache<String, EnergyStorageDimension> cache;
    private JedisPool jedisPool;

    public AsyncRedisLookupFunctionSingleNode(String redisPassword) {
        this.redisPassword = redisPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(50); // 设置最大连接数
        config.setMaxIdle(6);  // 设置最大空闲连接数
        config.setMinIdle(5);   // 设置最小空闲连接数
        config.setMaxWaitMillis(10000); // 设置最大等待时间（毫秒）
        config.setTestOnBorrow(true);  // 在借用连接之前进行测试
        config.setTestOnReturn(true);  // 在归还连接之前进行测试
        config.setTestWhileIdle(true); // 在空闲时测试连接

        String redisHost = "10.10.62.21"; // 单个Redis节点的主机名
        int redisPort = 6379; // 单个Redis节点的端口号

        jedisPool = new JedisPool(config, redisHost, redisPort, 10000, redisPassword);
        jedis = jedisPool.getResource();

        this.executorService = Executors.newFixedThreadPool(1);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedis != null) {
            jedis.close();
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Override
    public void asyncInvoke(StatMutation input, ResultFuture<List<EnrichedStatMutation>> resultFuture) {
        CompletableFuture.supplyAsync(() -> {

            Jedis jedis = jedisPool.getResource();
            List<EnergyStorageDimension> energyStorageDimensions = fetchData(input.getStation(), input.getEmu_sn(), input.getCabinet_no(), input.getName(),jedis);
            List<EnrichedStatMutation> enrichedStatMutations = new ArrayList<>();
            for (EnergyStorageDimension energyStorageDimension : energyStorageDimensions) {
                EnrichedStatMutation enrichedStatMutation = ParseValueUtil.createEnrichedStatMutation(input, energyStorageDimension);
                enrichedStatMutations.add(enrichedStatMutation);
            }
            return enrichedStatMutations;
        }, executorService).thenAccept(result -> {
            if (result != null && !result.isEmpty()) {
                resultFuture.complete(Collections.singletonList(result));
            } else {
                resultFuture.complete(Collections.emptyList());
            }
        });
    }

    private static long parseLong(String value) {
        return value == null ? 0L : Long.parseLong(value);
    }

    private static BigDecimal parseBigDecimal(String value) {
        if (value == null || value.isEmpty() || !isNumeric(value)) {
            return BigDecimal.ZERO;
        }
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            System.err.println("Invalid BigDecimal value: " + value);
            e.printStackTrace(); // 打印堆栈信息以便调试
            return BigDecimal.ZERO; // 或者根据你的业务逻辑选择其他默认值
        }
    }

    private static boolean isNumeric(String str) {
        return NumberUtils.isCreatable(str);
    }

    private long calculateMeasNo(long time) {
        return Timestamp.valueOf("1970-01-01 00:00:00").toLocalDateTime()
                .until(Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime(), ChronoUnit.HOURS);
    }

    public List<EnergyStorageDimension> fetchData(String stationId, String emuSn, String cabinetNo, String paramSn,Jedis jedis) {
        List<EnergyStorageDimension> result = new ArrayList<>();
        List<Map.Entry<String, Map<String, String>>> equLogicEquData = fetchEquLogicEquData(stationId, emuSn, cabinetNo, jedis);
        Map<String, Map<String, String>> equStationDataMap = new HashMap<>();
        Map<String, Map<String, String>> equStationTypeDataMap = new HashMap<>();
        Map<String, List<Map.Entry<String, Map<String, String>>>> equStationAttrDataMap = new HashMap<>();
        Map<String, List<Map.Entry<String, Map<String, String>>>> equAggrStationRelateDataMap = new HashMap<>();
        Map<String, Map<String, String>> equAggrStationDataMap = new HashMap<>();

        for (Map.Entry<String, Map<String, String>> entry : equLogicEquData) {
            Map<String, String> logicEqu = entry.getValue();
            String logicEquId = logicEqu.get("logic_equ_id");

            if (logicEquId != null) {
                Map<String, String> paramData = fetchParamData(logicEquId, paramSn,jedis);
                logicEqu.putAll(paramData);

                String stationIdFromLogicEqu = logicEqu.get("station_id");
                if (!equStationDataMap.containsKey(stationIdFromLogicEqu)) {
                    equStationDataMap.put(stationIdFromLogicEqu, fetchStationData(stationIdFromLogicEqu,jedis));
                }

                String stationTypeId = equStationDataMap.get(stationIdFromLogicEqu).get("station_type_id");
                if (!equStationTypeDataMap.containsKey(stationTypeId)) {
                    equStationTypeDataMap.put(stationTypeId, fetchStationTypeData(stationTypeId,jedis));
                }

                if (!equStationAttrDataMap.containsKey(stationIdFromLogicEqu)) {
                    equStationAttrDataMap.put(stationIdFromLogicEqu, fetchStationAttrData(stationIdFromLogicEqu, "StaCapacity",jedis));
                }

                if (!equAggrStationRelateDataMap.containsKey(stationIdFromLogicEqu)) {
                    equAggrStationRelateDataMap.put(stationIdFromLogicEqu, fetchAggrStationRelateData(stationIdFromLogicEqu,jedis));
                }

                for (Map.Entry<String, Map<String, String>> aggrRelateEntry : equAggrStationRelateDataMap.get(stationIdFromLogicEqu)) {
                    Map<String, String> aggrRelate = aggrRelateEntry.getValue();
                    String aggrStationId = aggrRelate.get("aggr_station_id");
                    if (!equAggrStationDataMap.containsKey(aggrStationId)) {
                        equAggrStationDataMap.put(aggrStationId, fetchAggrStationData(aggrStationId,jedis));
                    }
                }
            }
        }

        for (Map.Entry<String, Map<String, String>> entry : equLogicEquData) {
            Map<String, String> logicEqu = entry.getValue();
            String stationIdFromLogicEqu = logicEqu.get("station_id");
            String stationTypeId = equStationDataMap.get(stationIdFromLogicEqu).get("station_type_id");

            logicEqu.putAll(equStationDataMap.get(stationIdFromLogicEqu));
            logicEqu.putAll(equStationTypeDataMap.get(stationTypeId));

            List<Map.Entry<String, Map<String, String>>> stationAttrs = equStationAttrDataMap.getOrDefault(stationIdFromLogicEqu, Collections.emptyList());
            List<Map.Entry<String, Map<String, String>>> aggrStationRelates = equAggrStationRelateDataMap.getOrDefault(stationIdFromLogicEqu, Collections.emptyList());

            if (stationAttrs.isEmpty() && aggrStationRelates.isEmpty()) {
                EnergyStorageDimension dimension = mapToEnergyStorageDimension(logicEqu);
                result.add(dimension);
            } else if (stationAttrs.isEmpty()) {
                for (Map.Entry<String, Map<String, String>> aggrRelateEntry : aggrStationRelates) {
                    Map<String, String> aggrRelate = aggrRelateEntry.getValue();
                    logicEqu.putAll(aggrRelate);
                    String aggrStationId = aggrRelate.get("aggr_station_id");
                    if (equAggrStationDataMap.containsKey(aggrStationId)) {
                        logicEqu.putAll(equAggrStationDataMap.get(aggrStationId));
                    }
                    EnergyStorageDimension dimension = mapToEnergyStorageDimension(logicEqu);
                    result.add(dimension);
                }
            } else if (aggrStationRelates.isEmpty()) {
                for (Map.Entry<String, Map<String, String>> stationAttrEntry : stationAttrs) {
                    Map<String, String> stationAttr = stationAttrEntry.getValue();
                    logicEqu.putAll(stationAttr);
                    EnergyStorageDimension dimension = mapToEnergyStorageDimension(logicEqu);
                    result.add(dimension);
                }
            } else {
                for (Map.Entry<String, Map<String, String>> stationAttrEntry : stationAttrs) {
                    Map<String, String> stationAttr = stationAttrEntry.getValue();
                    for (Map.Entry<String, Map<String, String>> aggrRelateEntry : aggrStationRelates) {
                        Map<String, String> aggrRelate = aggrRelateEntry.getValue();
                        logicEqu.putAll(stationAttr);
                        logicEqu.putAll(aggrRelate);
                        String aggrStationId = aggrRelate.get("aggr_station_id");
                        if (equAggrStationDataMap.containsKey(aggrStationId)) {
                            logicEqu.putAll(equAggrStationDataMap.get(aggrStationId));
                        }
                        EnergyStorageDimension dimension = mapToEnergyStorageDimension(logicEqu);
                        result.add(dimension);
                    }
                }
            }
        }

        return result;
    }

    public List<Map.Entry<String, Map<String, String>>> fetchEquLogicEquData(String stationId, String emuSn, String cabinetNo, Jedis jedis) {
        List<Map.Entry<String, Map<String, String>>> filteredEntries = new ArrayList<>();
         {
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams().match("equ_logic_equ:*").count(100);
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getStringCursor();
                for (String key : scanResult.getResult()) {
                    Map<String, String> hash = jedis.hgetAll(key);
                    if (stationId.equals(hash.get("station_id")) && emuSn.equals(hash.get("emu_sn")) && cabinetNo.equals(hash.get("cabinet_no"))) {
                        filteredEntries.add(new AbstractMap.SimpleEntry<>(key, hash));
                        System.out.println("Matching entry found for key: " + key);
                    }
                }
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        }
        return filteredEntries;
    }

    public Map<String, String> fetchParamData(String logicEquId, String paramSn ,Jedis jedis) {
        Map<String, String> result = new HashMap<>();
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams().match("equ_le_param:*").count(100);
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getStringCursor();
                for (String key : scanResult.getResult()) {
                    Map<String, String> hash = jedis.hgetAll(key);
                    if (logicEquId.equals(hash.get("logic_equ_id")) && paramSn.equals(hash.get("param_name"))) {
                        System.out.println("Matching entry found for key: " + key);
                        return hash;
                    }
                }
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));

        return result;
    }

    private Map<String, String> fetchStationData(String stationId,Jedis jedis) {
          String redisKey = "equ_station:" + stationId;
            return jedis.hgetAll(redisKey);
        }


    private Map<String, String> fetchStationTypeData(String stationTypeId,Jedis jedis) {
        String redisKey = "equ_station_type:" + stationTypeId;
        return jedis.hgetAll(redisKey);
    }

    public List<Map.Entry<String, Map<String, String>>> fetchStationAttrData(String stationId, String attrCode,Jedis jedis) {
        List<Map.Entry<String, Map<String, String>>> filteredEntries = new ArrayList<>();
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams().match("equ_station_attr:*").count(100);
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getStringCursor();
                for (String key : scanResult.getResult()) {
                    Map<String, String> hash = jedis.hgetAll(key);
                    if (stationId.equals(hash.get("station_id"))) {
                        if (attrCode.equals(hash.get("attr_code"))) {
                            filteredEntries.add(new AbstractMap.SimpleEntry<>(key, hash));
                            System.out.println("StaCapacity attr found for key: " + key);
                        }
                    }
                }
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        return filteredEntries;
    }

    private List<Map.Entry<String, Map<String, String>>> fetchAggrStationRelateData(String stationId,Jedis jedis) {
        List<Map.Entry<String, Map<String, String>>> result = new ArrayList<>();

            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams().match("equ_aggr_station_relate:*").count(100);
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                cursor = scanResult.getStringCursor();
                for (String key : scanResult.getResult()) {
                    Map<String, String> hash = jedis.hgetAll(key);
                    if (stationId.equals(hash.get("station_id"))) {
                        result.add(new AbstractMap.SimpleEntry<>(key, hash));
                        System.out.println("Matching entry found for key: " + key);
                    }
                }
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        return result;
    }

    private Map<String, String> fetchAggrStationData(String aggrStationId,Jedis jedis) {
        String redisKey = "equ_aggr_station:" + aggrStationId;
            return jedis.hgetAll(redisKey);
    }

    private EnergyStorageDimension mapToEnergyStorageDimension(Map<String, String> data) {
        EnergyStorageDimension dimension = new EnergyStorageDimension();
        dimension.setMeasuringId(data.get("measuring_id"));
        dimension.setAggrStationId(parseLong(data.get("aggr_station_id")));
        dimension.setAggrStationCode(data.get("aggr_station_code"));
        dimension.setAggrStationName(data.get("aggr_station_name"));
        dimension.setStationId(parseLong(data.get("station_id")));
        dimension.setStationCode(data.get("station_code"));
        dimension.setStationName(data.get("station_name"));
        dimension.setStationAbbr(data.get("station_abbr"));
        dimension.setStationTypeId(parseLong(data.get("station_type_id")));
        dimension.setStationTypeCode(data.get("station_type_code"));
        dimension.setInterStation(data.get("inter_station"));
        dimension.setStaCapacity(parseBigDecimal(data.get("sta_capacity")));
        dimension.setTypeId(parseLong(data.get("type_id")));
        dimension.setTypeCode(data.get("type_code"));
        dimension.setTypeName(data.get("type_name"));
        dimension.setLogicEquId(parseLong(data.get("logic_equ_id")));
        dimension.setLogicEquCode(data.get("logic_equ_code"));
        dimension.setLogicEquName(data.get("logic_equ_name"));
        dimension.setIndicatorTempId(parseLong(data.get("indicator_temp_id")));
        dimension.setModel(data.get("model"));
        dimension.setInterEqu(data.get("inter_equ"));
        dimension.setParamSn(data.get("param_sn"));
        dimension.setParamId(parseLong(data.get("param_id")));
        dimension.setParamCode(data.get("param_code"));
        dimension.setParamType(data.get("param_type"));
        dimension.setParamName(data.get("param_name"));
        dimension.setParamClaz(data.get("param_claz"));
        dimension.setCoef(parseBigDecimal(data.get("coef")));
        dimension.setAlmClaz(data.get("alm_claz"));
        dimension.setAlmLevel(data.get("alm_level"));
        dimension.setNoAlm(parseBoolean(data.get("no_alm")));
        dimension.setFaultMonitor(parseBoolean(data.get("fault_monitor")));
        dimension.setMainAdvise(data.get("main_advise"));
        dimension.setRangeUpper(parseBigDecimal(data.get("range_upper")));
        dimension.setRangeLower(parseBigDecimal(data.get("range_lower")));
        dimension.setInvalidValue(data.get("invalid_value"));
        dimension.setExpValue(data.get("exp_value"));
        dimension.setRecovery(parseBoolean(data.get("recovery")));
        dimension.setStatus(data.get("status"));
        dimension.setMsgRuleId(parseLong(data.get("msg_rule_id")));
        dimension.setEmuSn(data.get("emu_sn"));
        dimension.setCabinetNo(data.get("cabinet_no"));
        dimension.setTenantId(parseLong(data.get("tenant_id")));
        dimension.setScript(data.get("script"));
        dimension.setRelateParamCode(data.get("relate_param_code"));
        dimension.setCustView(parseBoolean(data.get("cust_view")));
        dimension.setCustAlmName(data.get("cust_alm_name"));
        return dimension;
    }
}

