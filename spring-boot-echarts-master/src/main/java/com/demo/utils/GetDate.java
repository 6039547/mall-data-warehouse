package com.demo.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class GetDate {
    public static String getSysDate(){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");//设置日期格式
        return df.format(new Date());// new Date()为获取当前系统时间
    }
}
