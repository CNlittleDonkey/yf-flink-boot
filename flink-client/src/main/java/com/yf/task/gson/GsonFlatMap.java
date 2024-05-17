package com.yf.task.gson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import com.yf.bean.FlowData;
import com.yf.bean.SourceData;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.LinkedList;


public class GsonFlatMap extends RichFlatMapFunction<String, Tuple2<SourceData, FlowData>> {

    private static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    /**
     * 记录接收的数据的数量
     */
    private IntCounter numLines = new IntCounter();


    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext().addAccumulator("num-GsonFlatMap", this.numLines);

    }

    @Override
    public void flatMap(String value, Collector<Tuple2<SourceData, FlowData>> out) {

        LinkedList<SourceData> dataLinkedList = gson.fromJson(value, new TypeToken<LinkedList<SourceData>>() {
        }.getType());

        LinkedList<FlowData> flowDataLinkedList = gson.fromJson(value, new TypeToken<LinkedList<FlowData>>() {
        }.getType());

        for (int i = 0; i < dataLinkedList.size(); i++) {
            this.numLines.add(1);
            SourceData sourceData = dataLinkedList.get(i);
            sourceData.setThreadName(Thread.currentThread().getName());
            FlowData flowData = flowDataLinkedList.get(i);
            out.collect(Tuple2.of(sourceData, flowData));
        }

    }
}
