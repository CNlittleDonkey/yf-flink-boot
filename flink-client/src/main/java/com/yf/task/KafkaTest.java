package com.yf.task;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yf.task.pojo.StatMutation;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

/**
 * @ClassName KafkaTest
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/28 17:35
 * @Version 1.0
 */
public class KafkaTest {
    public static void main(String[] args) throws Exception {
        // 设置 Flink 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 Kafka 属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.10.5.152:9092,10.10.5.151:9092,10.10.5.150:9092");
        properties.setProperty("group.id", "flink-group");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty("auto.offset.reset", "earliest");

        // 创建 Kafka Source
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("de_cloud_stream_stat_mutation_measuring", new SimpleStringSchema(), properties);
        kafkaSource.setStartFromEarliest();

        // 从 Kafka 读取数据流
        DataStream<String> kafkaStream = env.addSource(kafkaSource);

        // 解析 JSON 数组数据并转换为 StatMutation 对象流
        DataStream<StatMutation> mutationDataStream = kafkaStream.flatMap(new FlatMapFunction<String, StatMutation>() {
            @Override
            public void flatMap(String value, Collector<StatMutation> out) throws Exception {
                // 处理包含 JSON 数组的字符串
                ObjectMapper objectMapper = new ObjectMapper();
                List<StatMutation> statMutations = objectMapper.readValue(value, new TypeReference<List<StatMutation>>() {});
                for (StatMutation statMutation : statMutations) {
                    out.collect(statMutation);
                }
            }
        });

        // 打印解析后的数据以验证
        mutationDataStream.print();

        // 执行 Flink 作业
        env.execute("Flink Kafka to StatMutation");
    }


}
