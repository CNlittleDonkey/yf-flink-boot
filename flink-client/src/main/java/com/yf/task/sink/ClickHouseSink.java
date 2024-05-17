package com.yf.task.sink;

import com.yf.service.ClickHouseDataService;
import com.yf.task.source.ClickHouseDataSourceConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;


public class ClickHouseSink extends RichSinkFunction<Tuple2<String, String>> {
    private transient ClickHouseDataService ClickHouseDataService;
    private transient AnnotationConfigApplicationContext context;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        context = new AnnotationConfigApplicationContext(ClickHouseDataSourceConfig.class);
        ClickHouseDataService = context.getBean(ClickHouseDataService.class);
    }

    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        ClickHouseDataService.insertData(value.f0, value.f1);
    }

    @Override
    public void close() throws Exception {
        if (context != null) {
            context.close();
        }
    }

}
