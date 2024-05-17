package com.yf.task;


import com.yf.env.BaseFlink;
import com.yf.task.simple.SimpleFunction;
import com.yf.task.sink.ClickHouseSink;
import com.yf.task.source.SimpleDataSource;
import com.yf.until.PropertiesUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;

public class KafkaClient  extends BaseFlink  {


    @Override
    public String getJobName() {
        return "kafka-calc";
    }

    @Override
    public String getConfigName() {
        return "topology-clickhouse.xml";
    }

    @Override
    public String getPropertiesName() {
        return "config.properties";
    }

    public static void main(String[] args) throws Exception {
        KafkaClient topo = new KafkaClient();
        topo.run(ParameterTool.fromArgs(args));
    }
    @Override
    public void createTopology(StreamExecutionEnvironment builder) throws IOException {

        DataStream<String> inputDataStrem = getKafkaSpout(properties.getProperty("read-topic").trim());
        SingleOutputStreamOperator<Tuple2<String, String>> parsedStream = inputDataStrem.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                ObjectMapper jsonParser = new ObjectMapper();
                JsonNode jsonNode = jsonParser.readTree(value);
                String key1 = jsonNode.get("param_sn").asText();
                String key2 = jsonNode.get("param_value").asText();
                return new Tuple2<>(key1, key2);
            }
        });
        ClickHouseSink clickHouseSink = new ClickHouseSink();
        parsedStream.addSink(clickHouseSink);
    }
}
