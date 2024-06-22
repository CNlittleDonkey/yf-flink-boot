package com.yf.task.pojo;

import java.math.BigDecimal;

/**
 * @ClassName StatMutation
 * @Description TODO
 * @Author xuhaoYF501492
 * @Date 2024/6/20 17:07
 * @Version 1.0
 */
public class StatMutation {
    private String station;
    private String cabinet;
    private String emuSn;
    private String name;
    private BigDecimal raw;
    private long time;
    public StatMutation(String station, String cabinet, String emuSn, String name, BigDecimal raw, long time) {
        this.station = station;
        this.cabinet = cabinet;
        this.emuSn = emuSn;
        this.name = name;
        this.raw = raw;
        this.time = time;
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public String getCabinet() {
        return cabinet;
    }

    public void setCabinet(String cabinet) {
        this.cabinet = cabinet;
    }

    public String getEmuSn() {
        return emuSn;
    }

    public void setEmuSn(String emuSn) {
        this.emuSn = emuSn;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public BigDecimal getRaw() {
        return raw;
    }

    public void setRaw(BigDecimal raw) {
        this.raw = raw;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
