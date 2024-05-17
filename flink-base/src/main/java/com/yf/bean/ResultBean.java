package com.yf.bean;


public class ResultBean {

    private String flowId;

    private int cycleNumber;

    private long total;

    private int stepNumber;

    private String stepType;

    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    public int getCycleNumber() {
        return cycleNumber;
    }

    public void setCycleNumber(int cycleNumber) {
        this.cycleNumber = cycleNumber;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public int getStepNumber() {
        return stepNumber;
    }

    public void setStepNumber(int stepNumber) {
        this.stepNumber = stepNumber;
    }

    public String getStepType() {
        return stepType;
    }

    public void setStepType(String stepType) {
        this.stepType = stepType;
    }

    @Override
    public String toString() {
        return "ResultBean{" +
                "flowId='" + flowId + '\'' +
                ", cycleNumber=" + cycleNumber +
                ", total=" + total +
                ", stepNumber=" + stepNumber +
                ", stepType='" + stepType + '\'' +
                '}';
    }
}
