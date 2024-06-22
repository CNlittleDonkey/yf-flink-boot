package com.yf.task.pojo;

import java.math.BigDecimal;

/**
 * @ClassName ZipperMutation
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/20 17:21
 * @Version 1.0
 */
public class ZipperMutation {
    private String measuringId;
    private Long aggrStationId;
    private String aggrStationCode;
    private String aggrStationName;
    private Long stationId;
    private String stationCode;
    private String stationName;
    private String stationAbbr;
    private String prodSeries;
    private String cabinetNo;
    private String emuSn;
    private BigDecimal staCapacity;
    private Long typeId;
    private String typeCode;
    private String typeName;
    private Long logicEquId;
    private String logicEquCode;
    private String logicEquName;
    private String interEqu;
    private Long qualityCode;
    private String paramSn;
    private Long paramId;
    private String paramCode;
    private String paramName;
    private String paramType;
    private String paramClaz;
    private String almClaz;
    private String almLevel;
    private Boolean noAlm;
    private Boolean faultMonitor;
    private String mainAdvise;
    private BigDecimal paramValue;
    private BigDecimal coef;
    private Long measTime;
    private Boolean recovery;
    private String status;
    private Long tenantId;
    private Long msgRuleId;
    private BigDecimal paramCoefValue;
    private String script;
    private String relateParamCode;
    private Boolean custView;
    private String custAlmName;
    private Boolean testAlm;

    public ZipperMutation(String measuringId, Long aggrStationId, String aggrStationCode, String aggrStationName, Long stationId, String stationCode, String stationName, String stationAbbr, String prodSeries, String cabinetNo, String emuSn, BigDecimal staCapacity, Long typeId, String typeCode, String typeName, Long logicEquId, String logicEquCode, String logicEquName, String interEqu, Long qualityCode, String paramSn, Long paramId, String paramCode, String paramName, String paramType, String paramClaz, String almClaz, String almLevel, Boolean noAlm, Boolean faultMonitor, String mainAdvise, BigDecimal paramValue, BigDecimal coef, Long measTime, Boolean recovery, String status, Long tenantId, Long msgRuleId, BigDecimal paramCoefValue, String script, String relateParamCode, Boolean custView, String custAlmName, Boolean testAlm) {
        this.measuringId = measuringId;
        this.aggrStationId = aggrStationId;
        this.aggrStationCode = aggrStationCode;
        this.aggrStationName = aggrStationName;
        this.stationId = stationId;
        this.stationCode = stationCode;
        this.stationName = stationName;
        this.stationAbbr = stationAbbr;
        this.prodSeries = prodSeries;
        this.cabinetNo = cabinetNo;
        this.emuSn = emuSn;
        this.staCapacity = staCapacity;
        this.typeId = typeId;
        this.typeCode = typeCode;
        this.typeName = typeName;
        this.logicEquId = logicEquId;
        this.logicEquCode = logicEquCode;
        this.logicEquName = logicEquName;
        this.interEqu = interEqu;
        this.qualityCode = qualityCode;
        this.paramSn = paramSn;
        this.paramId = paramId;
        this.paramCode = paramCode;
        this.paramName = paramName;
        this.paramType = paramType;
        this.paramClaz = paramClaz;
        this.almClaz = almClaz;
        this.almLevel = almLevel;
        this.noAlm = noAlm;
        this.faultMonitor = faultMonitor;
        this.mainAdvise = mainAdvise;
        this.paramValue = paramValue;
        this.coef = coef;
        this.measTime = measTime;
        this.recovery = recovery;
        this.status = status;
        this.tenantId = tenantId;
        this.msgRuleId = msgRuleId;
        this.paramCoefValue = paramCoefValue;
        this.script = script;
        this.relateParamCode = relateParamCode;
        this.custView = custView;
        this.custAlmName = custAlmName;
        this.testAlm = testAlm;
    }

    public String getMeasuringId() {
        return measuringId;
    }

    public void setMeasuringId(String measuringId) {
        this.measuringId = measuringId;
    }

    public Long getAggrStationId() {
        return aggrStationId;
    }

    public void setAggrStationId(Long aggrStationId) {
        this.aggrStationId = aggrStationId;
    }

    public String getAggrStationCode() {
        return aggrStationCode;
    }

    public void setAggrStationCode(String aggrStationCode) {
        this.aggrStationCode = aggrStationCode;
    }

    public String getAggrStationName() {
        return aggrStationName;
    }

    public void setAggrStationName(String aggrStationName) {
        this.aggrStationName = aggrStationName;
    }

    public Long getStationId() {
        return stationId;
    }

    public void setStationId(Long stationId) {
        this.stationId = stationId;
    }

    public String getStationCode() {
        return stationCode;
    }

    public void setStationCode(String stationCode) {
        this.stationCode = stationCode;
    }

    public String getStationName() {
        return stationName;
    }

    public void setStationName(String stationName) {
        this.stationName = stationName;
    }

    public String getStationAbbr() {
        return stationAbbr;
    }

    public void setStationAbbr(String stationAbbr) {
        this.stationAbbr = stationAbbr;
    }

    public String getProdSeries() {
        return prodSeries;
    }

    public void setProdSeries(String prodSeries) {
        this.prodSeries = prodSeries;
    }

    public String getCabinetNo() {
        return cabinetNo;
    }

    public void setCabinetNo(String cabinetNo) {
        this.cabinetNo = cabinetNo;
    }

    public String getEmuSn() {
        return emuSn;
    }

    public void setEmuSn(String emuSn) {
        this.emuSn = emuSn;
    }

    public BigDecimal getStaCapacity() {
        return staCapacity;
    }

    public void setStaCapacity(BigDecimal staCapacity) {
        this.staCapacity = staCapacity;
    }

    public Long getTypeId() {
        return typeId;
    }

    public void setTypeId(Long typeId) {
        this.typeId = typeId;
    }

    public String getTypeCode() {
        return typeCode;
    }

    public void setTypeCode(String typeCode) {
        this.typeCode = typeCode;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public Long getLogicEquId() {
        return logicEquId;
    }

    public void setLogicEquId(Long logicEquId) {
        this.logicEquId = logicEquId;
    }

    public String getLogicEquCode() {
        return logicEquCode;
    }

    public void setLogicEquCode(String logicEquCode) {
        this.logicEquCode = logicEquCode;
    }

    public String getLogicEquName() {
        return logicEquName;
    }

    public void setLogicEquName(String logicEquName) {
        this.logicEquName = logicEquName;
    }

    public String getInterEqu() {
        return interEqu;
    }

    public void setInterEqu(String interEqu) {
        this.interEqu = interEqu;
    }

    public Long getQualityCode() {
        return qualityCode;
    }

    public void setQualityCode(Long qualityCode) {
        this.qualityCode = qualityCode;
    }

    public String getParamSn() {
        return paramSn;
    }

    public void setParamSn(String paramSn) {
        this.paramSn = paramSn;
    }

    public Long getParamId() {
        return paramId;
    }

    public void setParamId(Long paramId) {
        this.paramId = paramId;
    }

    public String getParamCode() {
        return paramCode;
    }

    public void setParamCode(String paramCode) {
        this.paramCode = paramCode;
    }

    public String getParamName() {
        return paramName;
    }

    public void setParamName(String paramName) {
        this.paramName = paramName;
    }

    public String getParamType() {
        return paramType;
    }

    public void setParamType(String paramType) {
        this.paramType = paramType;
    }

    public String getParamClaz() {
        return paramClaz;
    }

    public void setParamClaz(String paramClaz) {
        this.paramClaz = paramClaz;
    }

    public String getAlmClaz() {
        return almClaz;
    }

    public void setAlmClaz(String almClaz) {
        this.almClaz = almClaz;
    }

    public String getAlmLevel() {
        return almLevel;
    }

    public void setAlmLevel(String almLevel) {
        this.almLevel = almLevel;
    }

    public Boolean getNoAlm() {
        return noAlm;
    }

    public void setNoAlm(Boolean noAlm) {
        this.noAlm = noAlm;
    }

    public Boolean getFaultMonitor() {
        return faultMonitor;
    }

    public void setFaultMonitor(Boolean faultMonitor) {
        this.faultMonitor = faultMonitor;
    }

    public String getMainAdvise() {
        return mainAdvise;
    }

    public void setMainAdvise(String mainAdvise) {
        this.mainAdvise = mainAdvise;
    }

    public BigDecimal getParamValue() {
        return paramValue;
    }

    public void setParamValue(BigDecimal paramValue) {
        this.paramValue = paramValue;
    }

    public BigDecimal getCoef() {
        return coef;
    }

    public void setCoef(BigDecimal coef) {
        this.coef = coef;
    }

    public Long getMeasTime() {
        return measTime;
    }

    public void setMeasTime(Long measTime) {
        this.measTime = measTime;
    }

    public Boolean getRecovery() {
        return recovery;
    }

    public void setRecovery(Boolean recovery) {
        this.recovery = recovery;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public Long getMsgRuleId() {
        return msgRuleId;
    }

    public void setMsgRuleId(Long msgRuleId) {
        this.msgRuleId = msgRuleId;
    }

    public BigDecimal getParamCoefValue() {
        return paramCoefValue;
    }

    public void setParamCoefValue(BigDecimal paramCoefValue) {
        this.paramCoefValue = paramCoefValue;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    public String getRelateParamCode() {
        return relateParamCode;
    }

    public void setRelateParamCode(String relateParamCode) {
        this.relateParamCode = relateParamCode;
    }

    public Boolean getCustView() {
        return custView;
    }

    public void setCustView(Boolean custView) {
        this.custView = custView;
    }

    public String getCustAlmName() {
        return custAlmName;
    }

    public void setCustAlmName(String custAlmName) {
        this.custAlmName = custAlmName;
    }

    public Boolean getTestAlm() {
        return testAlm;
    }

    public void setTestAlm(Boolean testAlm) {
        this.testAlm = testAlm;
    }
}
