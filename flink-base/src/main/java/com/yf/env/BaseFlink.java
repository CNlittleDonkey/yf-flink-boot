package com.yf.env;

import com.yf.until.PropertiesUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Preconditions;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public abstract class BaseFlink {

    protected Configuration config = new Configuration();

    protected BeanFactory beanFactory;

    protected String configFile;

    protected String dubboConfigFile;

    protected StreamExecutionEnvironment env;

    //protected StreamTableEnvironment tableEnv;

    protected Properties properties;


    public void init(ParameterTool params) throws IOException {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
       // tableEnv = StreamTableEnvironment.create(env);

        this.properties = PropertiesUtils.getProperties(getPropertiesName());
        Configuration configuration = new Configuration();
        configuration.addAllToProperties(properties);
        env.getConfig().setGlobalJobParameters(configuration);

        String parallelism = params.get("parallelism");
        if (StringUtils.isNotBlank(parallelism)) {
            env.setParallelism(Integer.valueOf(parallelism));
        }

        String restartStrategy = params.get("restartStrategy");
        if ("fixedDelayRestart".equals(restartStrategy)) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                    3,
                    Time.of(60, TimeUnit.SECONDS)
            ));
        } else if ("noRestart".equals(restartStrategy)) {
            env.setRestartStrategy(RestartStrategies.noRestart());
        } else if ("fallBackRestart".equals(restartStrategy)) {
            env.setRestartStrategy(RestartStrategies.fallBackRestart());
        } else {
            env.setRestartStrategy(RestartStrategies.failureRateRestart(
                    3,
                    Time.of(5, TimeUnit.MINUTES),
                    Time.of(60, TimeUnit.SECONDS)
            ));
        }

        String isLocal = params.get("isLocal");
        if (StringUtils.isBlank(isLocal)) {
            String isIncremental = params.get("isIncremental");
            Preconditions.checkNotNull(isIncremental, "isIncremental is null");
            StateBackend stateBackend;
            String hadoopIp = properties.getProperty("hadoopIp");
            if ("isIncremental".equals(isIncremental)) {
                //如果本地调试，必须指定hdfs的端口信息，且要依赖hadoop包，如果集群执行，flink与hdfs在同一集群，那么可以不指定hdfs端口信息，也不用将hadoop打进jar包。
                stateBackend = new RocksDBStateBackend("hdfs:///home/intsmaze/flink/" + getJobName(), true);
                env.setStateBackend(stateBackend);
            } else if ("full".equals(isIncremental)) {
                stateBackend = new RocksDBStateBackend("hdfs://" + hadoopIp + "/home/intsmaze/flink/" + getJobName(), false);
                env.setStateBackend(stateBackend);
            }

            env.enableCheckpointing(5000);
            env.getCheckpointConfig().setCheckpointTimeout(30000);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
    }


    protected void setupConfig() {

        this.configFile = getConfigName();
        this.dubboConfigFile = getDubboConfigName();
        this.beanFactory = new BeanFactory(configFile);

        for (String key : this.properties.stringPropertyNames()) {
            this.config.setString(key, this.properties.getProperty(key));
        }
        this.config.setString(BeanFactory.SPRING_BEAN_FACTORY_XML, beanFactory.getXml());
        this.config.setString(BeanFactory.SPRING_BEAN_FACTORY_NAME, this.configFile);
        if(StringUtils.isNotBlank(dubboConfigFile))
        {
            this.config.setString(BeanFactory.DUBBO_SPRING_BEAN_FACTORY_NAME, this.dubboConfigFile);
        }
        env.getConfig().setGlobalJobParameters(this.config);

    }


    protected DataStream<String> getKafkaSpout(String topic) {
        String bootstrapServers = properties.getProperty("bootstrap.servers", "");
        String port = properties.getProperty("kafka.offset.Port", "");
        String id = StringUtils.join(getJobName(), "-", topic);
        Properties kafkaPro = new Properties();
        kafkaPro.setProperty("bootstrap.servers", bootstrapServers + ":" + port);
        kafkaPro.setProperty("group.id", id);
        System.out.println("-------------->>>>>>>>>>>>>>>>>> consumer name is :" + id);
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), kafkaPro);
        stringFlinkKafkaConsumer.setStartFromEarliest();

    /*    KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("10.10.5.152:9092")
                .setTopics("ibtes_cloud_collect_powertrain_battery")
                .setGroupId("flink-group-11")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
*/
        return  env.addSource(stringFlinkKafkaConsumer);
    }





    public abstract String getJobName();



    public abstract String getConfigName();

    public String getDubboConfigName()
    {
        return null;
    }


    public abstract String getPropertiesName();


    public abstract void createTopology(StreamExecutionEnvironment builder) throws IOException;


    public void run(ParameterTool params) throws Exception {

        init(params);
        setupConfig();
        createTopology(env);

        String topoName = StringUtils.join(getJobName(), "-", new DateTime().toString("yyyyMMdd-HHmmss"));
        env.execute(topoName);
    }

}
