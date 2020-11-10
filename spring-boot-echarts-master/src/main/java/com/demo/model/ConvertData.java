package com.demo.model;

import java.util.Date;

public class ConvertData {
    private Date dt;
    private int total_visitor_m_count;
    private int order_u_count;
    private double visitor2order_convert_ratio;
    private int payment_u_count;
    private double order2payment_convert_ratio;

    public Date getDt() {
        return dt;
    }

    public void setDt(Date dt) {
        this.dt = dt;
    }

    public int getTotal_visitor_m_count() {
        return total_visitor_m_count;
    }

    public void setTotal_visitor_m_count(int total_visitor_m_count) {
        this.total_visitor_m_count = total_visitor_m_count;
    }

    public int getOrder_u_count() {
        return order_u_count;
    }

    public void setOrder_u_count(int order_u_count) {
        this.order_u_count = order_u_count;
    }

    public double getVisitor2order_convert_ratio() {
        return visitor2order_convert_ratio;
    }

    public void setVisitor2order_convert_ratio(double visitor2order_convert_ratio) {
        this.visitor2order_convert_ratio = visitor2order_convert_ratio;
    }

    public int getPayment_u_count() {
        return payment_u_count;
    }

    public void setPayment_u_count(int payment_u_count) {
        this.payment_u_count = payment_u_count;
    }

    public double getOrder2payment_convert_ratio() {
        return order2payment_convert_ratio;
    }

    public void setOrder2payment_convert_ratio(double order2payment_convert_ratio) {
        this.order2payment_convert_ratio = order2payment_convert_ratio;
    }
}
