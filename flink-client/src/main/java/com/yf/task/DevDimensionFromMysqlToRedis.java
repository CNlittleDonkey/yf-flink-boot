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
 * @ClassName DevDimensionFromMysqlToRedis
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/27 8:37
 * @Version 1.0
 */
public class DevDimensionFromMysqlToRedis {
    public static void main(String[] args) throws Exception {
        // 创建Flink执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("zeroDateTimeBehavior", "convertToNull");
        jdbcProperties.put("decimal.handling.mode", "string");
        // 创建MySQL CDC Source
        MySqlSource<String> mySqlSource_equ_le_param = MySqlSource.<String>builder()
                .hostname("10.10.5.163")
                .port(33067)
                .databaseList("de_equ")
                .tableList("de_equ.equ_le_param")
                .username("gscndev")
                .password("YFgscn123..")
                .debeziumProperties(jdbcProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        // 从CDC Source读取数据
        //env.enableCheckpointing(3000);
        MySqlSource<String> mySqlSource_equ_logic_equ = MySqlSource.<String>builder()
                .hostname("10.10.5.163")
                .port(33067)
                .databaseList("de_equ")
                .tableList("de_equ.equ_logic_equ")
                .username("gscndev")
                .password("YFgscn123..")
                .debeziumProperties(jdbcProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        MySqlSource<String> mySqlSource_equ_station = MySqlSource.<String>builder()
                .hostname("10.10.5.163")
                .port(33067)
                .databaseList("de_equ")
                .tableList("de_equ.equ_station")
                .username("gscndev")
                .password("YFgscn123..")
                .debeziumProperties(jdbcProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        MySqlSource<String> mySqlSource_equ_type = MySqlSource.<String>builder()
                .hostname("10.10.5.163")
                .port(33067)
                .databaseList("de_equ")
                .tableList("de_equ.equ_type")
                .username("gscndev")
                .password("YFgscn123..")
                .debeziumProperties(jdbcProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();


        DataStream<String> mySQLSourceStream_equ_le_param = env.fromSource(mySqlSource_equ_le_param, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);

        DataStream<String> mySQLSourceStream_equ_logic_equ = env.fromSource(mySqlSource_equ_logic_equ, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);


        DataStream<String> mySQLSourceStream_equ_station = env.fromSource(mySqlSource_equ_station, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);

        DataStream<String> mySQLSourceStream_equ_type = env.fromSource(mySqlSource_equ_type, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);


        mySQLSourceStream_equ_type.print();

     // 处理数据并写入Redis
        mySQLSourceStream_equ_le_param.addSink(new EquLeParamRedisClusterSink("redis@hckj"));
        mySQLSourceStream_equ_logic_equ.addSink(new EquLogicEquRedisClusterSink("redis@hckj"));
        mySQLSourceStream_equ_station.addSink(new EquStationRedisClusterSink("redis@hckj"));
        mySQLSourceStream_equ_type.addSink(new EquTypeRedisClusterSink("redis@hckj"));
        env.execute("Flink CDC to Redis");
    }


}
