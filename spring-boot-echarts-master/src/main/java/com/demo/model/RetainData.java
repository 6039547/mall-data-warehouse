package com.demo.model;

import java.text.SimpleDateFormat;
import java.util.Date;

public class RetainData {

    private Date stat_date;
    private int new_mid_count;
    private double d1;
    private double d2;
    private double d3;
    private double d4;
    private double d5;
    private double d6;

    public String getStat_date() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(stat_date);
    }

    public void setStat_date(Date stat_date) {
        this.stat_date = stat_date;
    }

    public int getNew_mid_count() {
        return new_mid_count;
    }

    public void setNew_mid_count(int new_mid_count) {
        this.new_mid_count = new_mid_count;
    }

    public double getD1() {
        return d1;
    }

    public void setD1(double d1) {
        this.d1 = d1;
    }

    public double getD2() {
        return d2;
    }

    public void setD2(double d2) {
        this.d2 = d2;
    }

    public double getD3() {
        return d3;
    }

    public void setD3(double d3) {
        this.d3 = d3;
    }

    public double getD4() {
        return d4;
    }

    public void setD4(double d4) {
        this.d4 = d4;
    }

    public double getD5() {
        return d5;
    }

    public void setD5(double d5) {
        this.d5 = d5;
    }

    public double getD6() {
        return d6;
    }

    public void setD6(double d6) {
        this.d6 = d6;
    }
}
