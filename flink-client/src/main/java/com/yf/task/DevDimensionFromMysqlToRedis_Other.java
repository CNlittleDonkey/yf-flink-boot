package com.yf.task;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.yf.task.sink.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @ClassName DevDimensionFromMysqlToRedis_Other
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/27 14:28
 * @Version 1.0
 */
public class DevDimensionFromMysqlToRedis_Other {
    public static void main(String[] args) throws Exception {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("zeroDateTimeBehavior", "convertToNull");
        jdbcProperties.put("decimal.handling.mode", "string");
        // 创建MySQL CDC Source
        MySqlSource<String> mySqlSource_equ_station_attr = MySqlSource.<String>builder()
                .hostname("10.10.5.163")
                .port(33067)
                .databaseList("de_equ")
                .tableList("de_equ.equ_station_attr")
                .username("gscndev")
                .password("YFgscn123..")
                .debeziumProperties(jdbcProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        // 从CDC Source读取数据
        //env.enableCheckpointing(3000);
        MySqlSource<String> mySqlSource_equ_aggr_station_relate = MySqlSource.<String>builder()
                .hostname("10.10.5.163")
                .port(33067)
                .databaseList("de_equ")
                .tableList("de_equ.equ_aggr_station_relate")
                .username("gscndev")
                .password("YFgscn123..")
                .debeziumProperties(jdbcProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        MySqlSource<String> mySqlSource_equ_aggr_station = MySqlSource.<String>builder()
                .hostname("10.10.5.163")
                .port(33067)
                .databaseList("de_equ")
                .tableList("de_equ.equ_aggr_station")
                .username("gscndev")
                .password("YFgscn123..")
                .debeziumProperties(jdbcProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        MySqlSource<String> mySqlSource_equ_station_type = MySqlSource.<String>builder()
                .hostname("10.10.5.163")
                .port(33067)
                .databaseList("de_equ")
                .tableList("de_equ.equ_station_type")
                .username("gscndev")
                .password("YFgscn123..")
                .debeziumProperties(jdbcProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();


        DataStream<String> mySQLSourceStream_equ_station_attr = env.fromSource(mySqlSource_equ_station_attr, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);

        DataStream<String> mySQLSourceStream_equ_aggr_station_relate = env.fromSource(mySqlSource_equ_aggr_station_relate, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);


        DataStream<String> mySQLSourceStream_equ_aggr_station = env.fromSource(mySqlSource_equ_aggr_station, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);

        DataStream<String> mySQLSourceStream_equ_station_type = env.fromSource(mySqlSource_equ_station_type, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);


        // mySQLSourceStream.print();

        // 处理数据并写入Redis
        mySQLSourceStream_equ_station_attr.addSink(new EquStationAttrRedisClusterSink("redis@hckj"));
        mySQLSourceStream_equ_aggr_station_relate.addSink(new EquAggrStationRelateRedisClusterSink("redis@hckj"));
        mySQLSourceStream_equ_aggr_station.addSink(new EquAggrStationRedisClusterSink("redis@hckj"));
        mySQLSourceStream_equ_station_type.addSink(new EquStationTypeRedisClusterSink("redis@hckj"));
        env.execute("Flink CDC to Redis");
    }

}
