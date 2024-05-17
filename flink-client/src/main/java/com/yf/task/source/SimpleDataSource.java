package com.yf.task.source;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yf.bean.MainData;
import com.yf.transform.CommonDataSource;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;


public class SimpleDataSource extends CommonDataSource {

    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(SimpleDataSource.class);

    private static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    int seqId = 1;

    int num = 1;

    List<String> stepTypeList;

    List<String> subTestItemList;

    String billNumberPre = "intsmaze-";

    String barcodePre = "1992-";



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        stepTypeList = new ArrayList<String>();
        stepTypeList.add("点击");
        stepTypeList.add("浏览");
        stepTypeList.add("搜索");
        stepTypeList.add("点击");
        stepTypeList.add("浏览");

        subTestItemList = new ArrayList<String>();
        subTestItemList.add("测试方法-1");
        subTestItemList.add("测试方法-22");
        subTestItemList.add("测试方法-333");
        subTestItemList.add("测试方法-4444");
        subTestItemList.add("测试方法-55555");
        subTestItemList.add("测试方法-666666");
    }


    @Override
    public String sendMess() throws InterruptedException {
        MDC.put("traceID", "TraceId-" + UUID.randomUUID().toString());
        Thread.sleep(1000);
        seqId++;
        num++;
        if (num == 100) {
            num = 1;
        }
        Random ra = new Random();
        int index = ra.nextInt(subTestItemList.size());
        String subTestItem = subTestItemList.get(index);


        int stepIndex = seqId % stepTypeList.size();
        String stepType = stepTypeList.get(stepIndex);
        MainData mainData = new MainData();
        mainData.setBillNumber(billNumberPre + ((num + 1) * 11111));
        mainData.setBarcode(barcodePre + ((num + 1) * 11111));
        mainData.setSubTestItem(subTestItem);
        mainData.setFlowName("intsmaze");
        mainData.setStepType(stepType);
        mainData.setSeqId(seqId);
        mainData.setStepNumber(seqId % 28);
        mainData.setTestTime(new Timestamp(System.currentTimeMillis()));
        return gson.toJson(mainData);
    }
}
