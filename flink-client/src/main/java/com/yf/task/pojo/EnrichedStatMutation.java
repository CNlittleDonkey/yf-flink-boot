package com.yf.task.pojo;
import java.math.BigDecimal;


public class EnrichedStatMutation {
    private String measuringId;
    private Long aggrStationId;
    private String aggrStationCode;
    private String aggrStationName;
    private Long stationId;
    private String stationCode;
    private String stationName;
    private String stationAbbr;
    private Long stationTypeId;
    private String stationTypeCode;
    private String interStation;
    private String cabinetNo;
    private BigDecimal staCapacity;
    private Long typeId;
    private String typeCode;
    private String typeName;
    private Long logicEquId;
    private String logicEquCode;
    private String logicEquName;
    private String deviceSn;
    private Long indicatorTempId;
    private String model;
    private Boolean noAlm;
    private String mainAdvise;
    private String script;
    private String relateParamCode;
    private String interEqu;
    private Boolean custView;
    private String custAlmName;
    private String invalidValue;
    private BigDecimal rangeUpper;
    private BigDecimal rangeLower;
    private Long msgRuleId;
    private String almClaz;
    private String almLevel;
    private Boolean faultMonitor;
    private Long measNo;
    private String paramSn;
    private Long paramId;
    private String paramCode;
    private String paramType;
    private String paramName;
    private String paramClaz;
    private BigDecimal paramValue;
    private BigDecimal coef;
    private Long measTime;
    private Boolean recovery;
    private String status;
    private Long tenantId;

    // Getters and Setters


    public EnrichedStatMutation() {
    }

    public EnrichedStatMutation(String measuringId, Long aggrStationId, String aggrStationCode, String aggrStationName, Long stationId, String stationCode, String stationName, String stationAbbr, Long stationTypeId, String stationTypeCode, String interStation, String cabinetNo, BigDecimal staCapacity, Long typeId, String typeCode, String typeName, Long logicEquId, String logicEquCode, String logicEquName, String deviceSn, Long indicatorTempId, String model, Boolean noAlm, String mainAdvise, String script, String relateParamCode, String interEqu, Boolean custView, String custAlmName, String invalidValue, BigDecimal rangeUpper, BigDecimal rangeLower, Long msgRuleId, String almClaz, String almLevel, Boolean faultMonitor, Long measNo, String paramSn, Long paramId, String paramCode, String paramType, String paramName, String paramClaz, BigDecimal paramValue, BigDecimal coef, Long measTime, Boolean recovery, String status, Long tenantId) {
        this.measuringId = measuringId;
        this.aggrStationId = aggrStationId;
        this.aggrStationCode = aggrStationCode;
        this.aggrStationName = aggrStationName;
        this.stationId = stationId;
        this.stationCode = stationCode;
        this.stationName = stationName;
        this.stationAbbr = stationAbbr;
        this.stationTypeId = stationTypeId;
        this.stationTypeCode = stationTypeCode;
        this.interStation = interStation;
        this.cabinetNo = cabinetNo;
        this.staCapacity = staCapacity;
        this.typeId = typeId;
        this.typeCode = typeCode;
        this.typeName = typeName;
        this.logicEquId = logicEquId;
        this.logicEquCode = logicEquCode;
        this.logicEquName = logicEquName;
        this.deviceSn = deviceSn;
        this.indicatorTempId = indicatorTempId;
        this.model = model;
        this.noAlm = noAlm;
        this.mainAdvise = mainAdvise;
        this.script = script;
        this.relateParamCode = relateParamCode;
        this.interEqu = interEqu;
        this.custView = custView;
        this.custAlmName = custAlmName;
        this.invalidValue = invalidValue;
        this.rangeUpper = rangeUpper;
        this.rangeLower = rangeLower;
        this.msgRuleId = msgRuleId;
        this.almClaz = almClaz;
        this.almLevel = almLevel;
        this.faultMonitor = faultMonitor;
        this.measNo = measNo;
        this.paramSn = paramSn;
        this.paramId = paramId;
        this.paramCode = paramCode;
        this.paramType = paramType;
        this.paramName = paramName;
        this.paramClaz = paramClaz;
        this.paramValue = paramValue;
        this.coef = coef;
        this.measTime = measTime;
        this.recovery = recovery;
        this.status = status;
        this.tenantId = tenantId;
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

    public Long getStationTypeId() {
        return stationTypeId;
    }

    public void setStationTypeId(Long stationTypeId) {
        this.stationTypeId = stationTypeId;
    }

    public String getStationTypeCode() {
        return stationTypeCode;
    }

    public void setStationTypeCode(String stationTypeCode) {
        this.stationTypeCode = stationTypeCode;
    }

    public String getInterStation() {
        return interStation;
    }

    public void setInterStation(String interStation) {
        this.interStation = interStation;
    }

    public String getCabinetNo() {
        return cabinetNo;
    }

    public void setCabinetNo(String cabinetNo) {
        this.cabinetNo = cabinetNo;
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

    public String getDeviceSn() {
        return deviceSn;
    }

    public void setDeviceSn(String deviceSn) {
        this.deviceSn = deviceSn;
    }

    public Long getIndicatorTempId() {
        return indicatorTempId;
    }

    public void setIndicatorTempId(Long indicatorTempId) {
        this.indicatorTempId = indicatorTempId;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public Boolean getNoAlm() {
        return noAlm;
    }

    public void setNoAlm(Boolean noAlm) {
        this.noAlm = noAlm;
    }

    public String getMainAdvise() {
        return mainAdvise;
    }

    public void setMainAdvise(String mainAdvise) {
        this.mainAdvise = mainAdvise;
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

    public String getInterEqu() {
        return interEqu;
    }

    public void setInterEqu(String interEqu) {
        this.interEqu = interEqu;
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

    public String getInvalidValue() {
        return invalidValue;
    }

    public void setInvalidValue(String invalidValue) {
        this.invalidValue = invalidValue;
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

    public Long getMsgRuleId() {
        return msgRuleId;
    }

    public void setMsgRuleId(Long msgRuleId) {
        this.msgRuleId = msgRuleId;
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

    public Boolean getFaultMonitor() {
        return faultMonitor;
    }

    public void setFaultMonitor(Boolean faultMonitor) {
        this.faultMonitor = faultMonitor;
    }

    public Long getMeasNo() {
        return measNo;
    }

    public void setMeasNo(Long measNo) {
        this.measNo = measNo;
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
}

