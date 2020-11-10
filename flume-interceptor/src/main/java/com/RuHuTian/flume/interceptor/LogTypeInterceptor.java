package com.RuHuTian.flume.interceptor;


import org.apache.commons.lang.CharSet;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;


import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * @Description: 单个even处理
     * @param event
     * @return: org.apache.flume.Event
     * @Author: RuHuTian
     * @Date: 2020/09/24 14:26
     */
    @Override
    public Event intercept(Event event) {
        Map<String, String> header = event.getHeaders();
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));
        if(log.contains("start")){
            header.put("topic","topic_start");
        }else{
            header.put("topic","topic_event");
        }
        return event;
    }

    /**
     * @Description: 批量处理
     * @param list
     * @return: java.util.List<org.apache.flume.Event>
     * @Author: RuHuTian
     * @Date: 2020/09/24 14:27
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> eventList = new ArrayList<>();
        for (Event e : list){
            Event event = intercept(e);
            eventList.add(event);
        }
        return eventList;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
