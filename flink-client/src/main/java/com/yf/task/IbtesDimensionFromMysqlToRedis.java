package com.yf.task;




import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.yf.task.sink.RedisSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;


/**
 * @ClassName IbtesDimensionFromMysqlToRedis
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/22 14:38
 * @Version 1.0
 */
public class IbtesDimensionFromMysqlToRedis {

        public static void main(String[] args) throws Exception {
            // 创建Flink执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            Properties jdbcProperties = new Properties();
            jdbcProperties.put("zeroDateTimeBehavior", "convertToNull");
            jdbcProperties.put("decimal.handling.mode", "string");
            // 创建MySQL CDC Source
            MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                    .hostname("10.10.5.163")
                    .port(33067)
                    .databaseList("de_equ")
                    .tableList("de_equ.de_energystorage_dimensions_table")
                    .username("gscndev")
                    .password("YFgscn123..")
                    .debeziumProperties(jdbcProperties)
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .startupOptions(StartupOptions.initial())
                    .build();
            // 从CDC Source读取数据
            //env.enableCheckpointing(3000);

            DataStream<String> mySQLSourceStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                    .setParallelism(1);

            mySQLSourceStream.print();

       // 处理数据并写入Redis
           mySQLSourceStream.addSink(new RedisSink("10.10.62.21", 6379,"redis@hckj"));

            env.execute("Flink CDC to Redis");
        }




}
