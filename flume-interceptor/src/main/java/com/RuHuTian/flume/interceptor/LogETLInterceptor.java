package com.RuHuTian.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    /**
     * @Description: 单个事件拦截
     * @param event
     * @return: org.apache.flume.Event
     * @Author: RuHuTian
     * @Date: 2020/09/24 10:54
     */
    @Override
    public Event intercept(Event event) {

        byte[] body = event.getBody();
        
        String log = new String(body, Charset.forName("UTF-8"));
        
        if(log.contains("start")){
            
            if(LogUtils.validateStart(log)) return event;
            
        }else{
            
            if(LogUtils.validateEvent(log)) return event;
            
        }
        
        return null;
    }

    /**
     * @Description: 批量事件拦截
     * @param list
     * @return: java.util.List<org.apache.flume.Event>
     * @Author: RuHuTian
     * @Date: 2020/09/24 10:59
     */
    @Override
    public List<Event> intercept(List<Event> list) {

        ArrayList<Event> events = new ArrayList<>();
        for(Event event : list){

            Event e = intercept(event);

            if(e!=null) events.add(e);

        }

        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
