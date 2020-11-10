package com.RuHuTian.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;
import sun.rmi.runtime.Log;

import java.awt.*;

public class LogUtils {
    /**
     * @Description:验证启动日志
     * @param log
     * @return: boolean
     * @Author: RuHuTian
     * @Date: 2020/09/24 10:35
     */
    public static boolean validateStart(String log) {
        if(log == null) return false;

        if(!log.trim().startsWith("{") || !log.trim().endsWith("}")) return false;

        return true;
    }

    /**
     * @Description: 验证事件日志
     * @param log
     * @return: boolean
     * @Author: RuHuTian
     * @Date: 2020/09/24 10:44
     */
    public static boolean validateEvent(String log) {

        String[] logs = log.split("\\|");

        if(logs.length!=2) return false;

        if(logs[0].length()!=13 || !NumberUtils.isDigits(logs[0])) return false;

        if(!logs[1].trim().startsWith("{") || !logs[1].trim().endsWith("}")) return false;

        return true;
    }
}
