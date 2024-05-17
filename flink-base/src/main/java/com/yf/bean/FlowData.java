package com.yf.bean;


public class FlowData {

    private String uuid;
    private String subTestItem;
    private String billNumber;

    private String barcode;

    private String flowName;

    private String flowStatus;

    public String getFlowStatus() {
        return flowStatus;
    }

    public void setFlowStatus(String flowStatus) {
        this.flowStatus = flowStatus;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSubTestItem() {
        return subTestItem;
    }

    public void setSubTestItem(String subTestItem) {
        this.subTestItem = subTestItem;
    }

    public String getBillNumber() {
        return billNumber;
    }

    public void setBillNumber(String billNumber) {
        this.billNumber = billNumber;
    }

    public String getBarcode() {
        return barcode;
    }

    public void setBarcode(String barcode) {
        this.barcode = barcode;
    }

    public String getFlowName() {
        return flowName;
    }

    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    @Override
    public String toString() {
        return "FlowData{" +
                "uuid='" + uuid + '\'' +
                ", subTestItem='" + subTestItem + '\'' +
                ", billNumber='" + billNumber + '\'' +
                ", barcode='" + barcode + '\'' +
                ", flowName='" + flowName + '\'' +
                ", flowStatus='" + flowStatus + '\'' +
                '}';
    }
}
