package com.demo.model;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ActiveData {

    private Date date;
    private int activeCount;
    private String is_weekend;
    private String is_monthend;

    public String getIs_weekend() {
        return is_weekend;
    }

    public void setIs_weekend(String is_weekend) {
        this.is_weekend = is_weekend;
    }

    public String getIs_monthend() {
        return is_monthend;
    }

    public void setIs_monthend(String is_monthend) {
        this.is_monthend = is_monthend;
    }

    public String getDate() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(date);
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public int getActiveCount() {
        return activeCount;
    }

    public void setActiveCount(int activeCount) {
        this.activeCount = activeCount;
    }
}
