package com.yf.transform;


import com.yf.env.BeanFactory;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.springframework.context.ApplicationContext;

import java.util.Map;

public abstract class CommonDataSource extends RichParallelSourceFunction<String> {


    protected ApplicationContext beanFactory;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ExecutionConfig.GlobalJobParameters globalJobParameters = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        beanFactory = BeanFactory.getBeanFactory((Configuration)globalJobParameters);
    }


    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (true) {
            sourceContext.collect(sendMess());
        }
    }


    public abstract String sendMess() throws Exception;



    @Override
    public void cancel() {

    }
}
