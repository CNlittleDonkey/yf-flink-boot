package com.yf.task.pojo;

import java.io.Serializable;
import java.math.BigDecimal;

public class StatMutation implements Serializable {
    private long time;
    private String station;
    private String emu_sn;
    private String cabinet_no;
    private String name;
    private BigDecimal raw;

    // Getters and setters

    public StatMutation() {
    }

    public StatMutation(long time, String station, String emu_sn, String cabinet_no, String name, BigDecimal raw) {
        this.time = time;
        this.station = station;
        this.emu_sn = emu_sn;
        this.cabinet_no = cabinet_no;
        this.name = name;
        this.raw = raw;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public String getEmu_sn() {
        return emu_sn;
    }

    public void setEmu_sn(String emu_sn) {
        this.emu_sn = emu_sn;
    }

    public String getCabinet_no() {
        return cabinet_no;
    }

    public void setCabinet_no(String cabinet_no) {
        this.cabinet_no = cabinet_no;
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
}
