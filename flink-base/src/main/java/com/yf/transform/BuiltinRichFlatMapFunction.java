package com.yf.transform;


import com.yf.env.BeanFactory;
import com.yf.service.ClickHouseDataService;
import com.yf.service.DataService;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.springframework.context.ApplicationContext;


public abstract class BuiltinRichFlatMapFunction extends RichFlatMapFunction<String, String> {


    /**
     * 记录接收的数据的数量
     */
    private IntCounter numLines = new IntCounter();


    protected DataService dataService;

    protected ApplicationContext beanFactory;


    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext().addAccumulator("num-FlatMap", this.numLines);

        ExecutionConfig.GlobalJobParameters globalJobParameters = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        beanFactory = BeanFactory.getBeanFactory((Configuration) globalJobParameters);

        dataService = beanFactory.getBean(DataService.class);
    }


    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        this.numLines.add(1);
        String execute = execute(value);
        if (StringUtils.isNotBlank(execute)) {
            out.collect(execute);
        }
    }


    public abstract String execute(String message) throws Exception;

}
