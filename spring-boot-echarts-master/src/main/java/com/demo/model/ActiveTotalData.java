package com.demo.model;

public class ActiveTotalData {

    private String id;
    private String title;
    private int totalData;
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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getTotalData() {
        return totalData;
    }

    public void setTotalData(int totalData) {
        this.totalData = totalData;
    }
}
