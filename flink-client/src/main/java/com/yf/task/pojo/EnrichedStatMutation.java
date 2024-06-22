package com.yf.task.pojo;

import com.yf.until.ContainFun;

import java.math.BigDecimal;

/**
 * @ClassName EnrichedStatMutation
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/21 8:56
 * @Version 1.0
 */
public class EnrichedStatMutation {
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
    private Long paramId;
    private String paramCode;
    private String paramType;
    private String paramName;
    private String paramClaz;
    private BigDecimal coef;
    private String almClaz;
    private String almLevel;
    private Boolean noAlm;
    private Boolean faultMonitor;
    private String mainAdvise;
    private BigDecimal rangeUpper;
    private BigDecimal rangeLower;
    private String invalidValue;
    private String expValue;
    private Boolean recovery;
    private String status;
    private String paramSn;
    private BigDecimal paramValue;
    private BigDecimal paramCoefValue;
    private long measNo;
    private long measTime;
    private Long tenantId;
    private Long msgRuleId;
    private String script;
    private String relateParamCode;
    private Boolean custView;
    private String custAlmName;
    private Boolean testAlm;

    public EnrichedStatMutation(String measuringId, Long aggrStationId, String aggrStationCode, String aggrStationName, Long stationId, String stationCode, String stationName, String stationAbbr, String prodSeries, String cabinetNo, String emuSn, BigDecimal staCapacity, Long typeId, String typeCode, String typeName, Long logicEquId, String logicEquCode, String logicEquName, String interEqu, Long paramId, String paramCode, String paramType, String paramName, String paramClaz, BigDecimal coef, String almClaz, String almLevel, Boolean noAlm, Boolean faultMonitor, String mainAdvise, BigDecimal rangeUpper, BigDecimal rangeLower, String invalidValue, String expValue, Boolean recovery, String status, String paramSn, BigDecimal paramValue, BigDecimal paramCoefValue, long measNo, long measTime, Long tenantId, Long msgRuleId, String script, String relateParamCode, Boolean custView, String custAlmName, Boolean testAlm) {
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
        this.paramId = paramId;
        this.paramCode = paramCode;
        this.paramType = paramType;
        this.paramName = paramName;
        this.paramClaz = paramClaz;
        this.coef = coef;
        this.almClaz = almClaz;
        this.almLevel = almLevel;
        this.noAlm = noAlm;
        this.faultMonitor = faultMonitor;
        this.mainAdvise = mainAdvise;
        this.rangeUpper = rangeUpper;
        this.rangeLower = rangeLower;
        this.invalidValue = invalidValue;
        this.expValue = expValue;
        this.recovery = recovery;
        this.status = status;
        this.paramSn = paramSn;
        this.paramValue = paramValue;
        this.paramCoefValue = paramCoefValue;
        this.measNo = measNo;
        this.measTime = measTime;
        this.tenantId = tenantId;
        this.msgRuleId = msgRuleId;
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

    public String getParamType() {
        return paramType;
    }

    public void setParamType(String paramType) {
        this.paramType = paramType;
    }

    public String getParamName() {
        return paramName;
    }

    public void setParamName(String paramName) {
        this.paramName = paramName;
    }

    public String getParamClaz() {
        return paramClaz;
    }

    public void setParamClaz(String paramClaz) {
        this.paramClaz = paramClaz;
    }

    public BigDecimal getCoef() {
        return coef;
    }

    public void setCoef(BigDecimal coef) {
        this.coef = coef;
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

    public BigDecimal getRangeUpper() {
        return rangeUpper;
    }

    public void setRangeUpper(BigDecimal rangeUpper) {
        this.rangeUpper = rangeUpper;
    }

    public BigDecimal getRangeLower() {
        return rangeLower;
    }

    public void setRangeLower(BigDecimal rangeLower) {
        this.rangeLower = rangeLower;
    }

    public String getInvalidValue() {
        return invalidValue;
    }

    public void setInvalidValue(String invalidValue) {
        this.invalidValue = invalidValue;
    }

    public String getExpValue() {
        return expValue;
    }

    public void setExpValue(String expValue) {
        this.expValue = expValue;
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

    public String getParamSn() {
        return paramSn;
    }

    public void setParamSn(String paramSn) {
        this.paramSn = paramSn;
    }

    public BigDecimal getParamValue() {
        return paramValue;
    }

    public void setParamValue(BigDecimal paramValue) {
        this.paramValue = paramValue;
    }

    public BigDecimal getParamCoefValue() {
        return paramCoefValue;
    }

    public void setParamCoefValue(BigDecimal paramCoefValue) {
        this.paramCoefValue = paramCoefValue;
    }

    public long getMeasNo() {
        return measNo;
    }

    public void setMeasNo(long measNo) {
        this.measNo = measNo;
    }

    public long getMeasTime() {
        return measTime;
    }

    public void setMeasTime(long measTime) {
        this.measTime = measTime;
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
