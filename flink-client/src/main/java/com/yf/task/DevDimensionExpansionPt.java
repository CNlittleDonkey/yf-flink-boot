package com.yf.task;

import com.alibaba.fastjson2.JSONObject;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.JsonNode;

import com.yf.env.BaseFlink;
import com.yf.task.pojo.EnrichedStatMutation;
import com.yf.task.pojo.StatMutation;
import com.yf.task.simple.AsynTest;
import com.yf.task.simple.AsyncRedisLookupFunction;
import com.yf.task.simple.AsyncRedisLookupFunctionMutil;
import com.yf.task.simple.AsyncRedisLookupFunctionSingleNode;
import com.yf.until.ContainFun;
import com.yf.until.PropertiesUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName DevDimensionExpansionPt
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/27 16:26
 * @Version 1.0
 */
public class DevDimensionExpansionPt extends BaseFlink {

    @Override
    public String getJobName() {
        return "ibtes_dimension_expansion_pt";
    }

    @Override
    public String getConfigName() {
        return "topology-clickhouse.xml";

    }

    @Override
    public String getPropertiesName() {
        return "config.properties";

    }

    @Override
    public void createTopology(StreamExecutionEnvironment builder) throws Exception {

    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment  env = StreamExecutionEnvironment.getExecutionEnvironment();
        // tableEnv = StreamTableEnvironment.create(env);
        Properties properties = PropertiesUtils.getProperties("config.properties");
        Configuration configuration = new Configuration();
        configuration.addAllToProperties(properties);
        env.getConfig().setGlobalJobParameters(configuration);
        String bootstrapServers = properties.getProperty("bootstrap.servers", "");
        String inputTopic = properties.getProperty("inputTopic", "");
        String groupId = properties.getProperty("groupId", "");
        // String outputTopic = properties.getProperty("outputTopic", "");
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("de_cloud_stream_stat_mutation_measuring")
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
              /*  .setProperty("scan.topic-partition-discovery.interval", "24h")
                .setProperty("json.fail-on-missing-field", "true")
                .setProperty("json.ignore-parse-errors", "true")*/
                .build();
        StatMutation statMutation = new StatMutation(1698656120277L, "720", "SN01", "04", "运行状态", new BigDecimal(110.0));
        DataStream<StatMutation> mainStream = env.addSource(new SourceFunction<StatMutation>() {
            private boolean running = true;

            @Override
            public void run(SourceContext<StatMutation> ctx) throws Exception {
                while (running) {
                    ctx.collect(statMutation);
                    Thread.sleep(1000); // 每秒生成一个数据
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        // 从 Kafka 读取数据流
       // DataStreamSource<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //System.out.println(kafkaStream);
        //kafkaStream.print();
        // 从 Kafka 读取数据流
       // DataStreamSource<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");


      // 解析 JSON 数组数据并转换为 StatMutation 对象
        // 解析 JSON 数组数据并转换为 StatMutation 对象流
  /*    DataStream<StatMutation> mainStream = kafkaStream.flatMap(new FlatMapFunction<String, StatMutation>() {
            @Override
            public void flatMap(String value, Collector<StatMutation> out) throws Exception {
                // 处理包含 JSON 数组的字符串
                ObjectMapper objectMapper = new ObjectMapper();
                List<StatMutation> statMutations = objectMapper.readValue(value, new TypeReference<List<StatMutation>>() {});
                for (StatMutation statMutation : statMutations) {
                    out.collect(statMutation);
                }
            }
        });*/

        // 打印解析后的数据以验证
        mainStream.print();
       // DataStreamSource<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");


        // 创建主数据流
     /*   SingleOutputStreamOperator<StatMutation> mainStream = kafkaStream.map(
                        (MapFunction<String, StatMutation>) value -> {
                            try {
                                return JSONObject.parseObject(value, StatMutation.class);
                            } catch (Exception e) {
                                return null;
                            }
                        }).filter(Objects::nonNull)
                .returns(StatMutation.class);*/


        // 连接主数据流和异步 Redis 查找函数
        SingleOutputStreamOperator<EnrichedStatMutation> resultStream = AsyncDataStream.unorderedWait(
                mainStream,
                new AsyncRedisLookupFunctionSingleNode("redis@hckj"),
                1000,
                TimeUnit.MILLISECONDS,
                100
        ).flatMap(new FlatMapFunction<List<EnrichedStatMutation>, EnrichedStatMutation>() {
            @Override
            public void flatMap(List<EnrichedStatMutation> value, Collector<EnrichedStatMutation> out) throws Exception {
                for (EnrichedStatMutation enrichedStatMutation : value) {
                    out.collect(enrichedStatMutation);
                }
            }
        });

// 打印结果流的内容以进行调试
        resultStream.print();


        String insertQuery = "INSERT INTO test_A (" +
                "measuring_id, aggr_station_id, aggr_station_code, aggr_station_name, station_id, station_code, station_name, station_abbr, " +
                "station_type_id, station_type_code, inter_station, cabinet_no, sta_capacity, type_id, type_code, type_name, logic_equ_id, logic_equ_code, logic_equ_name, " +
                "device_sn, indicator_temp_id, model, inter_equ, meas_no, quality_code, param_sn, param_id, param_code, param_type, param_name, param_claz, " +
                "param_value, param_coef_value, coef, meas_time, recovery, status, create_time, tenant_id) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        resultStream.addSink(
                JdbcSink.sink(
                        insertQuery,
                        (statement, enrichedStatMutation) -> {
                            statement.setString(1, enrichedStatMutation.getMeasuringId());
                            statement.setLong(2, enrichedStatMutation.getAggrStationId());
                            statement.setString(3, enrichedStatMutation.getAggrStationCode());
                            statement.setString(4, enrichedStatMutation.getAggrStationName());
                            statement.setLong(5, enrichedStatMutation.getStationId());
                            statement.setString(6, enrichedStatMutation.getStationCode());
                            statement.setString(7, enrichedStatMutation.getStationName());
                            statement.setString(8, enrichedStatMutation.getStationAbbr());
                            statement.setLong(9, enrichedStatMutation.getStationTypeId());
                            statement.setString(10, enrichedStatMutation.getStationTypeCode());
                            statement.setString(11, enrichedStatMutation.getInterStation());
                            statement.setString(12, enrichedStatMutation.getCabinetNo());
                            statement.setBigDecimal(13, enrichedStatMutation.getStaCapacity());
                            statement.setLong(14, enrichedStatMutation.getTypeId());
                            statement.setString(15, enrichedStatMutation.getTypeCode());
                            statement.setString(16, enrichedStatMutation.getTypeName());
                            statement.setLong(17, enrichedStatMutation.getLogicEquId());
                            statement.setString(18, enrichedStatMutation.getLogicEquCode());
                            statement.setString(19, enrichedStatMutation.getLogicEquName());
                            statement.setString(20, enrichedStatMutation.getDeviceSn());
                            statement.setLong(21, enrichedStatMutation.getIndicatorTempId());
                            statement.setString(22, enrichedStatMutation.getModel());
                            statement.setString(23, enrichedStatMutation.getInterEqu());
                            statement.setLong(24, enrichedStatMutation.getMeasNo());

                            // Calculate quality_code
                            BigDecimal paramValue = enrichedStatMutation.getParamValue();
                            String invalidValue = enrichedStatMutation.getInvalidValue();
                            BigDecimal rangeUpper = enrichedStatMutation.getRangeUpper();
                            BigDecimal rangeLower = enrichedStatMutation.getRangeLower();

                            int qualityCode = 0;
                            if (ContainFun.valFun(paramValue.toString(), invalidValue)) {
                                qualityCode = 2;
                            } else if (rangeUpper != null && paramValue.compareTo(rangeUpper) > 0 || rangeLower != null && paramValue.compareTo(rangeLower) < 0) {
                                qualityCode = 1;
                            } else if (!ContainFun.valFun(paramValue.toString(), invalidValue) || ((rangeUpper != null && paramValue.compareTo(rangeUpper) <= 0 && rangeLower != null && paramValue.compareTo(rangeLower) >= 0) || (rangeUpper == null && rangeLower == null))) {
                                qualityCode = 0;
                            } else {
                                qualityCode = 3;
                            }
                            statement.setInt(25, qualityCode);

                            statement.setString(26, enrichedStatMutation.getParamSn());
                            statement.setLong(27, enrichedStatMutation.getParamId());
                            statement.setString(28, enrichedStatMutation.getParamCode());
                            statement.setString(29, enrichedStatMutation.getParamType());
                            statement.setString(30, enrichedStatMutation.getParamName());
                            statement.setString(31, enrichedStatMutation.getParamClaz());
                            statement.setBigDecimal(32, enrichedStatMutation.getParamValue());
                            statement.setBigDecimal(33, enrichedStatMutation.getParamValue().multiply(enrichedStatMutation.getCoef()));
                            statement.setBigDecimal(34, enrichedStatMutation.getCoef());
                            statement.setTimestamp(35, new Timestamp(enrichedStatMutation.getMeasTime()));
                            statement.setBoolean(36, enrichedStatMutation.getRecovery());
                            statement.setString(37, enrichedStatMutation.getStatus());
                            statement.setTimestamp(38, new Timestamp(System.currentTimeMillis()));
                            statement.setLong(39, enrichedStatMutation.getTenantId());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://10.10.5.151:8127/de_cloud")
                                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                                .withUsername("default")
                                .withPassword("123456")
                                .build()
                ));
        String topoName = StringUtils.join("Dev_dimension_expansion_pt", "-", new DateTime().toString("yyyyMMdd-HHmmss"));
        env.execute(topoName);
    }

}
