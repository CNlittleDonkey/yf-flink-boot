package com.yf.task;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.yf.env.BaseFlink;
import com.yf.task.pojo.EnergyStorageDimension;
import com.yf.task.pojo.EnrichedStatMutation;
import com.yf.task.pojo.StatMutation;
import com.yf.task.simple.AsyncRedisLookupFunction;
import com.yf.until.ContainFun;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName Ibtes_Dimension_Expansion_pt
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/20 14:39
 * @Version 1.0
 */
public class IbtesDimensionExpansionPt extends BaseFlink  {

    @Override
    public String getJobName() {
        return "ibtes_dimension_expansion_pt";
    }

    @Override
    public String getConfigName() {
        return null;
    }

    @Override
    public String getPropertiesName() {
        return null;
    }

    @Override
    public void createTopology(StreamExecutionEnvironment builder) throws Exception {

        String bootstrapServers = properties.getProperty("bootstrap.servers", "");
        String inputTopic = properties.getProperty("inputTopic", "");
        String groupId = properties.getProperty("groupId","");
        String outputTopic = properties.getProperty("outputTopic", "");
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setProperty("scan.topic-partition-discovery.interval", "24h")
                .setProperty("json.fail-on-missing-field", "true")
                .setProperty("json.ignore-parse-errors", "true")
                .build();
        DataStreamSource<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");


   /*     // 定义广播状态描述符
        MapStateDescriptor<String, EnergyStorageDimension> broadcastStateDescriptor =
                new MapStateDescriptor<>(
                        "broadcast-state",
                        TypeInformation.of(String.class),
                        TypeInformation.of(new TypeHint<EnergyStorageDimension>() {
                        })
                );*/

        // 创建主数据流

        SingleOutputStreamOperator<StatMutation> mainStream = kafkaStream.map(
                        (MapFunction<String, StatMutation>) value -> {
                            try {
                                return JSONObject.parseObject(value, StatMutation.class);
                            } catch (Exception e) {
                                return null;
                            }
                        }).filter(Objects::nonNull)
                .returns(Types.POJO(StatMutation.class));

        // 连接主数据流和异步 Redis 查找函数
        SingleOutputStreamOperator<EnrichedStatMutation> resultStream = AsyncDataStream.unorderedWait(
                mainStream,
                new AsyncRedisLookupFunction("localhost", 6379),
                1000,
                TimeUnit.MILLISECONDS,
                100
        ).returns(Types.POJO(EnrichedStatMutation.class));  // 确保返回类型正确

        // 插入到 ClickHouse
        // 插入到 ClickHouse
        String insertQuery = "INSERT INTO dwd_equ_cdc_meas (" +
                "measuring_id, aggr_station_id, aggr_station_code, aggr_station_name, station_id, station_code, station_name, station_abbr, " +
                "prod_series, cabinet_no, emu_sn, sta_capacity, type_id, type_code, type_name, logic_equ_id, logic_equ_code, logic_equ_name, inter_equ, " +
                "meas_no, quality_code, param_sn, param_id, param_code, param_name, param_type, param_claz, param_value, param_coef_value, coef, " +
                "meas_time, recovery, status, create_time, tenant_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
                            statement.setString(9, enrichedStatMutation.getProdSeries());
                            statement.setString(10, enrichedStatMutation.getCabinetNo());
                            statement.setString(11, enrichedStatMutation.getEmuSn());
                            statement.setBigDecimal(12, enrichedStatMutation.getStaCapacity());
                            statement.setLong(13, enrichedStatMutation.getTypeId());
                            statement.setString(14, enrichedStatMutation.getTypeCode());
                            statement.setString(15, enrichedStatMutation.getTypeName());
                            statement.setLong(16, enrichedStatMutation.getLogicEquId());
                            statement.setString(17, enrichedStatMutation.getLogicEquCode());
                            statement.setString(18, enrichedStatMutation.getLogicEquName());
                            statement.setString(19, enrichedStatMutation.getInterEqu());
                            statement.setLong(20, enrichedStatMutation.getMeasNo());
                            statement.setInt(21, ContainFun.calculateQualityCode(enrichedStatMutation.getParamValue(),enrichedStatMutation.getInvalidValue(),enrichedStatMutation.getRangeUpper(),enrichedStatMutation.getRangeLower()));
                            statement.setString(22, enrichedStatMutation.getParamSn());
                            statement.setLong(23, enrichedStatMutation.getParamId());
                            statement.setString(24, enrichedStatMutation.getParamCode());
                            statement.setString(25, enrichedStatMutation.getParamName());
                            statement.setString(26, enrichedStatMutation.getParamType());
                            statement.setString(27, enrichedStatMutation.getParamClaz());
                            statement.setBigDecimal(28, enrichedStatMutation.getParamValue());
                            statement.setBigDecimal(29, enrichedStatMutation.getParamCoefValue());
                            statement.setBigDecimal(30, enrichedStatMutation.getCoef());
                            statement.setTimestamp(31, new Timestamp(enrichedStatMutation.getMeasTime()));
                            statement.setBoolean(32, enrichedStatMutation.getRecovery());
                            statement.setString(33, enrichedStatMutation.getStatus());
                            statement.setTimestamp(34, new Timestamp(System.currentTimeMillis()));
                            statement.setLong(35, enrichedStatMutation.getTenantId());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://localhost:8123/your_database")
                                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                                .withUsername("your_username")
                                .withPassword("your_password")
                                .build()
                ));

        env.execute("Kafka to ClickHouse");
    }
}
