package com.yf.task.simple;


import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.*;
import com.alibaba.ververica.connector.redis.shaded.redis.clients.jedis.exceptions.JedisMovedDataException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.yf.task.filter.FetchDataFromRedisUtils;
import com.yf.task.filter.ParseValueUtil;
import com.yf.task.pojo.EnergyStorageDimension;
import com.yf.task.pojo.EnrichedStatMutation;
import com.yf.task.pojo.PackedLogicEquAndParam;
import com.yf.task.pojo.StatMutation;
import com.yf.until.ContainFun;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class AsyncRedisLookupFunctionMutil extends RichAsyncFunction<StatMutation, List<EnrichedStatMutation>> {

    private final String redisPassword;
    private  transient JedisCluster jedisCluster;
    private transient ExecutorService executorService;
    private Map<String, JedisPool> clusterNodes;
    private transient Cache<String, EnergyStorageDimension> cache;

    public AsyncRedisLookupFunctionMutil(String redisPassword) {

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
        Set<HostAndPort> redisNodes = new HashSet<>();
        redisNodes.add(new HostAndPort("10.10.5.154", 6379));
        redisNodes.add(new HostAndPort("10.10.5.153", 6379));
        redisNodes.add(new HostAndPort("10.10.5.152", 6379));
        HostAndPort hostAndPort = new HostAndPort("10.10.5.154", 6379);
            // 使用带密码参数的构造函数初始化 JedisCluster
        jedisCluster = new JedisCluster(redisNodes,1000,1000, 5,redisPassword,config);
        clusterNodes = jedisCluster.getClusterNodes();
        System.out.println("JedisCluster initialized: " + jedisCluster);
        System.out.println("Cluster nodes: " + clusterNodes.keySet());


        this.executorService = Executors.newFixedThreadPool(1);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedisCluster != null) {
            try {
                jedisCluster.close();
                System.out.println("JedisCluster closed");
            } catch (Exception e) {
                System.err.println("Error closing JedisCluster: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    @Override
    public void asyncInvoke(StatMutation input, ResultFuture<List<EnrichedStatMutation>> resultFuture) {
        CompletableFuture.supplyAsync(() -> {
            clusterNodes = jedisCluster.getClusterNodes();

            if (clusterNodes == null || clusterNodes.isEmpty()) {
                System.err.println("xuhao11  Cluster nodes are empty or null");
            }
            // 在这里执行异步 Redis 查找并返回结果
            List<EnergyStorageDimension> energyStorageDimensions = fetchData(input.getStation(), input.getEmu_sn(), input.getCabinet_no(), input.getName());
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


    public List<EnergyStorageDimension> fetchData(String stationId, String emuSn, String cabinetNo, String paramSn) {
        List<EnergyStorageDimension> result = new ArrayList<>();

        List<Map.Entry<String, Map<String, String>>> equLogicEquData = fetchEquLogicEquData(stationId, emuSn, cabinetNo);
        Map<String, Map<String, String>> equStationDataMap = new HashMap<>();
        Map<String, Map<String, String>> equStationTypeDataMap = new HashMap<>();
        Map<String, List<Map.Entry<String, Map<String, String>>>> equStationAttrDataMap = new HashMap<>();
        Map<String, List<Map.Entry<String, Map<String, String>>>> equAggrStationRelateDataMap = new HashMap<>();
        Map<String, Map<String, String>> equAggrStationDataMap = new HashMap<>();

        // Fetch and store station data
        for (Map.Entry<String, Map<String, String>> entry : equLogicEquData) {
            Map<String, String> logicEqu = entry.getValue();
            String logicEquId = logicEqu.get("logic_equ_id");

            if (logicEquId != null) {
                Map<String, String> paramData = fetchParamData(logicEquId, paramSn);
                logicEqu.putAll(paramData);

                String stationIdFromLogicEqu = logicEqu.get("station_id");
                if (!equStationDataMap.containsKey(stationIdFromLogicEqu)) {
                    equStationDataMap.put(stationIdFromLogicEqu, fetchStationData(stationIdFromLogicEqu));
                }

                String stationTypeId = equStationDataMap.get(stationIdFromLogicEqu).get("station_type_id");
                if (!equStationTypeDataMap.containsKey(stationTypeId)) {
                    equStationTypeDataMap.put(stationTypeId, fetchStationTypeData(stationTypeId));
                }

                if (!equStationAttrDataMap.containsKey(stationIdFromLogicEqu)) {
                    equStationAttrDataMap.put(stationIdFromLogicEqu, fetchStationAttrData(stationIdFromLogicEqu, "StaCapacity"));
                }

                if (!equAggrStationRelateDataMap.containsKey(stationIdFromLogicEqu)) {
                    equAggrStationRelateDataMap.put(stationIdFromLogicEqu, fetchAggrStationRelateData(stationIdFromLogicEqu));
                }

                for (Map.Entry<String, Map<String, String>> aggrRelateEntry : equAggrStationRelateDataMap.get(stationIdFromLogicEqu)) {
                    Map<String, String> aggrRelate = aggrRelateEntry.getValue();
                    String aggrStationId = aggrRelate.get("aggr_station_id");
                    if (!equAggrStationDataMap.containsKey(aggrStationId)) {
                        equAggrStationDataMap.put(aggrStationId, fetchAggrStationData(aggrStationId));
                    }
                }
            }
        }

        // Combine all fetched data into EnergyStorageDimension instances
        for (Map.Entry<String, Map<String, String>> entry : equLogicEquData) {
            Map<String, String> logicEqu = entry.getValue();
            String stationIdFromLogicEqu = logicEqu.get("station_id");
            String stationTypeId = equStationDataMap.get(stationIdFromLogicEqu).get("station_type_id");

            logicEqu.putAll(equStationDataMap.get(stationIdFromLogicEqu));
            logicEqu.putAll(equStationTypeDataMap.get(stationTypeId));

            // Handle multiple station attributes and aggregate station relates
            List<Map.Entry<String, Map<String, String>>> stationAttrs = equStationAttrDataMap.getOrDefault(stationIdFromLogicEqu, Collections.emptyList());
            List<Map.Entry<String, Map<String, String>>> aggrStationRelates = equAggrStationRelateDataMap.getOrDefault(stationIdFromLogicEqu, Collections.emptyList());

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

        return result;
    }



  /*  public List<Map.Entry<String, Map<String, String>>> fetchEquLogicEquData(String stationId, String emuSn, String cabinetNo) {
        List<Map.Entry<String, Map<String, String>>> filteredEntries = new ArrayList<>();

        // 假设我们只选择第一个节点执行扫描操作
        Optional<Map.Entry<String, JedisPool>> firstNode = clusterNodes.entrySet().stream()
                .filter(entry -> entry.getKey().endsWith(":6379"))
                .findFirst();

        if (firstNode.isPresent()) {
            JedisPool jedisPool = firstNode.get().getValue();
            try (Jedis jedis = jedisPool.getResource()) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match("equ_logic_equ:*").count(100); // Count指定每次扫描的数量
                do {
                    ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                    cursor = scanResult.getStringCursor();
                    for (String key : scanResult.getResult()) {
                        Map<String, String> hash = jedisCluster.hgetAll(key);
                        //System.out.println("xuhao111"+hash.toString());
                        if (stationId.equals(hash.get("station_id")) && emuSn.equals(hash.get("emu_sn")) && cabinetNo.equals(hash.get("cabinet_no"))) {
                            filteredEntries.add(new AbstractMap.SimpleEntry<>(key, hash));
                        }
                    }
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            } catch (Exception e) {
                System.err.println("Error accessing node " + firstNode.get().getKey() + ": " + e.getMessage());
            }
        } else {
            System.err.println("No cluster nodes available");
        }

        return filteredEntries;
    }*/

    public List<Map.Entry<String, Map<String, String>>> fetchEquLogicEquData(String stationId, String emuSn, String cabinetNo) {
        List<Map.Entry<String, Map<String, String>>> filteredEntries = new ArrayList<>();
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();

        for (Map.Entry<String, JedisPool> entry : clusterNodes.entrySet()) {
            String nodeAddress = entry.getKey();
            JedisPool jedisPool = entry.getValue();
            try (Jedis jedis = jedisPool.getResource()) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match("equ_logic_equ:*").count(100); // Count指定每次扫描的数量
                do {
                    try {
                        ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                        cursor = scanResult.getStringCursor();
                        for (String key : scanResult.getResult()) {
                            Map<String, String> hash = jedis.hgetAll(key);
                            // 根据 stationId、emuSn 和 cabinetNo 进行过滤
                            if (stationId.equals(hash.get("station_id")) && emuSn.equals(hash.get("emu_sn")) && cabinetNo.equals(hash.get("cabinet_no"))) {
                                filteredEntries.add(new AbstractMap.SimpleEntry<>(key, hash));
                                System.out.println("Matching entry found for key: " + key);
                            }
                        }
                    } catch (JedisMovedDataException e) {
                        System.err.println("MOVED data error: " + e.getMessage() + ". Retrying with new node.");
                        cursor = ScanParams.SCAN_POINTER_START; // reset cursor
                        // The moved data exception will be handled by JedisCluster automatically, no need to retry manually.
                    }
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            } catch (Exception e) {
                System.err.println("Error accessing node " + nodeAddress + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        return filteredEntries;
    }

    public Map<String, String> fetchParamData(String logicEquId, String paramSn) {
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        Map<String, String> result = new HashMap<>();

        for (Map.Entry<String, JedisPool> entry : clusterNodes.entrySet()) {
            String nodeAddress = entry.getKey();
            JedisPool jedisPool = entry.getValue();
            try (Jedis jedis = jedisPool.getResource()) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match("equ_le_param:*").count(100); // Count指定每次扫描的数量
                do {
                    try {
                        ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                        cursor = scanResult.getStringCursor();
                        for (String key : scanResult.getResult()) {
                            Map<String, String> hash = jedis.hgetAll(key);
                            // 根据 logicEquId 和 paramSn 进行过滤
                            if (logicEquId.equals(hash.get("logic_equ_id")) && paramSn.equals(hash.get("param_name"))) {
                                System.out.println("Matching entry found for key: " + key);
                                return hash;
                            }
                        }
                    } catch (JedisMovedDataException e) {
                        System.err.println("MOVED data error: " + e.getMessage() + ". Retrying with new node.");
                        cursor = ScanParams.SCAN_POINTER_START; // reset cursor
                        // The moved data exception will be handled by JedisCluster automatically, no need to retry manually.
                    }
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            } catch (Exception e) {
                System.err.println("Error accessing node " + nodeAddress + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        return result;
    }


/*    public Map<String, String> fetchParamData(String logicEquId, String paramSn) {
        // 筛选出主节点（假设主节点端口是 6379）
        // 假设我们只选择第一个节点执行扫描操作
        Optional<Map.Entry<String, JedisPool>> firstNode = clusterNodes.entrySet().stream()
                .filter(entry -> entry.getKey().endsWith(":6379"))
                .findFirst();

        if (firstNode.isPresent()) {
            System.out.println("Using master node: " + firstNode.get().getKey());
            JedisPool jedisPool = firstNode.get().getValue();
            try (Jedis jedis = jedisPool.getResource()) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match("equ_le_param:*").count(1000); // Count指定每次扫描的数量
                do {
                    ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                    cursor = scanResult.getStringCursor();
                    for (String key : scanResult.getResult()) {
                        Map<String, String> hash = jedisCluster.hgetAll(key);
                        // 根据 logicEquId 和 paramSn 进行过滤
                        System.out.println("xuhao99999" +"  "+ logicEquId+"   "+paramSn);
                        //logicEquId.equals(hash.get("logic_equ_id")) &&
                        if ( "140".equals(hash.get("logic_equ_id"))&&"单体最高温度值".equals(hash.get("param_name"))) {
                            System.out.println("xuhao88888" +"  "+ hash.get("param_name"));
                            return hash;
                        }
                    }
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            } catch (Exception e) {
                System.err.println("Error accessing master node " + firstNode.get().getKey() + ": " + e.getMessage());
            }
        } else {
            System.err.println("No master node available");
        }

        return new HashMap<>();
    }*/

 /*   public List<Map.Entry<String, Map<String, String>>> fetchParamData(String logicEquId, String paramSn){
        List<Map.Entry<String, Map<String, String>>> filteredEntries = new ArrayList<>();
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();

        for (String nodeKey : clusterNodes.keySet()) {
            JedisPool jedisPool = clusterNodes.get(nodeKey);
            try (Jedis jedis = jedisPool.getResource()) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match("equ_le_param:*").count(100); // Count指定每次扫描的数量
                do {
                    ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                    cursor = scanResult.getStringCursor();
                    for (String key : scanResult.getResult()) {
                        Map<String, String> hash = jedisCluster.hgetAll(key);
                        // 根据 stationId、emuSn 和 cabinetNo 进行过滤
                        if ("运行状态".equals(hash.get("param_name"))&&"140".equals(hash.get("logic_equ_id"))) {
                            filteredEntries.add(new AbstractMap.SimpleEntry<>(key, hash));
                        }
                    }
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            }
        }

        return filteredEntries;
    }*/

    private Map<String, String> fetchStationData(String stationId) {
        String redisKey = "equ_station:" + stationId;
        return jedisCluster.hgetAll(redisKey);
    }



    private Map<String, String> fetchStationTypeData(String stationTypeId) {
        String redisKey = "equ_station_type:" + stationTypeId;
        return jedisCluster.hgetAll(redisKey);
    }


  /*  private List<Map<String, String>> fetchStationAttrData(String stationId, String attrCode) {
        List<Map<String, String>> result = new ArrayList<>();

        Optional<Map.Entry<String, JedisPool>> masterNode = clusterNodes.entrySet().stream()
                .filter(entry -> entry.getKey().endsWith(":6379"))
                .findFirst();

        if (masterNode.isPresent()) {
            System.out.println("Using master node: " + masterNode.get().getKey());
            JedisPool jedisPool = masterNode.get().getValue();
            try (Jedis jedis = jedisPool.getResource()) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match("equ_station_attr:*").count(1000); // Count指定每次扫描的数量
                do {
                    ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                    cursor = scanResult.getStringCursor();
                    for (String key : scanResult.getResult()) {
                        Map<String, String> hash = jedisCluster.hgetAll(key);
                        // 根据 stationId 和 attrCode 进行过滤
                        //&& attrCode.equals(hash.get("attr_code").trim())
                        if (stationId.equals(hash.get("station_id")) ) {
                            System.out.println("xuha0020202+"+hash.get("attr_code"));
                            if(attrCode.equals(hash.get("attr_code")))
                            //System.out.println("xuha0020202+"+hash.get("attr_code"));
                            result.add(hash);
                        }
                    }
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            } catch (Exception e) {
                System.err.println("Error accessing master node " + masterNode.get().getKey() + ": " + e.getMessage());
            }
        } else {
            System.err.println("No master node available");
        }

        return result;
    }*/

    public List<Map.Entry<String, Map<String, String>>> fetchStationAttrData(String stationId, String attrCode) {
        List<Map.Entry<String, Map<String, String>>> filteredEntries = new ArrayList<>();
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();

        for (Map.Entry<String, JedisPool> entry : clusterNodes.entrySet()) {
            String nodeAddress = entry.getKey();
            JedisPool jedisPool = entry.getValue();
            try (Jedis jedis = jedisPool.getResource()) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match("equ_station_attr:*").count(100); // Count指定每次扫描的数量
                do {
                    try {
                        ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                        cursor = scanResult.getStringCursor();
                        for (String key : scanResult.getResult()) {
                            Map<String, String> hash = jedis.hgetAll(key);
                            // 根据 stationId 和 attrCode 进行过滤
                            if (stationId.equals(hash.get("station_id"))) {
                                if (attrCode.equals(hash.get("attr_code"))) {
                                    filteredEntries.add(new AbstractMap.SimpleEntry<>(key, hash));
                                    System.out.println("StaCapacity attr found for key: " + key);
                                }
                            }
                        }
                    } catch (JedisMovedDataException e) {
                        System.err.println("MOVED data error: " + e.getMessage() + ". Retrying with new node.");
                        cursor = ScanParams.SCAN_POINTER_START; // reset cursor
                        // The moved data exception will be handled by JedisCluster automatically, no need to retry manually.
                    }
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            } catch (Exception e) {
                System.err.println("Error accessing node " + nodeAddress + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        return filteredEntries;
    }


    private List<Map.Entry<String, Map<String, String>>> fetchAggrStationRelateData(String stationId) {
        List<Map.Entry<String, Map<String, String>>> result = new ArrayList<>();
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();

        for (Map.Entry<String, JedisPool> entry : clusterNodes.entrySet()) {
            String nodeAddress = entry.getKey();
            JedisPool jedisPool = entry.getValue();
            try (Jedis jedis = jedisPool.getResource()) {
                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match("equ_aggr_station_relate:*").count(100); // Count指定每次扫描的数量
                do {
                    try {
                        ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                        cursor = scanResult.getStringCursor();
                        for (String key : scanResult.getResult()) {
                            Map<String, String> hash = jedis.hgetAll(key);
                            // 根据 stationId 进行过滤
                            if (stationId.equals(hash.get("station_id"))) {
                                result.add(new AbstractMap.SimpleEntry<>(key, hash));
                                System.out.println("Matching entry found for key: " + key);
                            }
                        }
                    } catch (JedisMovedDataException e) {
                        System.err.println("MOVED data error: " + e.getMessage() + ". Retrying with new node.");
                        cursor = ScanParams.SCAN_POINTER_START; // reset cursor
                        // The moved data exception will be handled by JedisCluster automatically, no need to retry manually.
                    }
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            } catch (Exception e) {
                System.err.println("Error accessing node " + nodeAddress + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        return result;
    }


    private Map<String, String> fetchAggrStationData(String aggrStationId) {
        String redisKey = "equ_aggr_station:" + aggrStationId;
        return jedisCluster.hgetAll(redisKey);
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
