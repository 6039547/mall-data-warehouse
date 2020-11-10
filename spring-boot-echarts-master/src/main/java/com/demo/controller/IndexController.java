package com.demo.controller;

import com.demo.service.IndexService;
import com.demo.utils.GetDate;
import com.demo.utils.HttpClientUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping(value = "/")
@PropertySource({"classpath:config/config.properties"})
public class IndexController {

    /*@Value("${my.totalUrl}")
    private String totalUrl;

    @Value("${my.hourUrl}")
    private String hourUrl;

    @Value("${my.esDataUrl}")
    private String esDataUrl;

    @Value("${my.sexUrl}")
    private String sexUrl;*/

    //页面显示
    @RequestMapping(value = "active", method = RequestMethod.GET)
    public String index() { return "active"; }

    @RequestMapping(value = "retain", method = RequestMethod.GET)
    public String retain() { return "retain"; }

    @RequestMapping(value = "gmv", method = RequestMethod.GET)
    public String gmv() { return "gmv"; }

    @RequestMapping(value = "convert", method = RequestMethod.GET)
    public String convert() { return "convert"; }

    @RequestMapping(value = "map", method = RequestMethod.GET)
    public String map() { return "map"; }
    /*@RequestMapping(value = "table", method = RequestMethod.GET)
    public String table() {
        return "table";
    }
    @RequestMapping(value = "map", method = RequestMethod.GET)
    public String map() {
        return "map";
    }*/


    /*@RequestMapping(value = "getTotal", method = RequestMethod.GET)
    @ResponseBody
    public String getTotal(){
        //String sysDate = GetDate.getSysDate();
        //return HttpClientUtil.doGet(totalUrl+"?date="+sysDate);
        String s = "[{'id':'l0','name':'每日统计','yesData':100,'toData':50},{'id':'l1','name':'每日日活','yesData':500,'toData':50},{'id':'l2','name':'活跃度','yesData':100,'toData':50},{'id':'l3','name':'啊啊啊','yesData':100,'toData':50}]";
        return s;
    }

    //获取统计数据
    @RequestMapping(value = "getAnalysisData", method = RequestMethod.GET)
    @ResponseBody
    public String getList(String tag) throws IOException {
        //hourUrl
        //System.out.println(tag);
        String s = "{'yesterday':{'00':110,'01':110,'02':25,'03':40,'07':87,'08':65,'09':32,'10':45,'11':35,'12':87,'13':34,'14':78,'15':54,'16':110,'17':100,'18':100,'19':56,'20':89,'21':88,'22':87,'23':86},'today':{'00':122,'01':122,'02':110,'03':105,'04':100,'05':46,'06':24,'07':24,'08':123,'09':221,'10':222,'11':100,'12':100,'13':78,'14':87,'15':67,'16':44,'17':77,'18':120,'19':100,'20':100,'21':108,'22':80,'23':55}}";
        *//*String sysDate = GetDate.getSysDate();
        return HttpClientUtil.doGet(hourUrl+"?id="+tag+"&&date="+sysDate);*//*
        return s;
    }
    double i = 25.8;
    @RequestMapping(value = "getData", method = RequestMethod.GET)
    @ResponseBody
    public String getData(HttpServletRequest req){

        String level = req.getParameter("level");
        String draw = req.getParameter("draw");
        String start = req.getParameter("start");
        String length = req.getParameter("length");
        //System.out.println(time+level+text);

        String time = req.getParameter("time");
        String text = req.getParameter("text");
        int d = Integer.parseInt(draw);
        int s = Integer.parseInt(start)+1;
        int l = Integer.parseInt(level);
        int size = Integer.parseInt(length);
        String sysDate = GetDate.getSysDate();
        String url = esDataUrl + "?startpage=" + s + "&&size=" + size;
        if(time!=null&&!"".equals(time)){
            url = url + "&&date="+time;
        }else {
            url = url + "&&date="+"2019-02-14";
        }
        if(text!=null&&!"".equals(text)){
            url = url + "&&keyword="+text;
        }
        i = i+10.1;
        System.out.println(url+"    "+i);
        //获取前台额外传递过来的查询条件
        *//*String extra_search = req.getParameter("extra_search");*//*
        *//*String json = HttpClientUtil.doGet(url);
        return "{'draw':"+draw+",'data':"+json+"}";*//*
        return "{'data':{'draw':"+draw+",'total':2000,'stat':[{'options':[{'name':'20岁以下','value':0.0},{'name':'20岁到30岁','value':"+i+"},{'name':'30岁以上','value':74.2}],'title':'用户年龄占比'},{'options':[{'name':'男','value':38.7},{'name':'女','value':61.3}],'title':'用户性别占比'}],'detail':[{'user_id':'123'," +
                "'sku_id':'123'," +
                "'user_gender':'M'," +
                "'user_age':12," +
                "'user_level':1," +
                "'order_price':123.2," +
                "'sku_name':'asdasfddsgdfgdfhfgdhgfhfghfghfghfghfghfgfghfgh'," +
                "'sku_tm_id':'234'," +
                "'sku_category1_id':'435'," +
                "'sku_category2_id':'3465'," +
                "'sku_category3_id':'56'," +
                "'sku_category1_name':'dfg'," +
                "'sku_category2_name':'hg'," +
                "'sku_category3_name':'bc'," +
                "'spu_id':'876'," +
                "'sku_num':234," +
                "'order_count':234," +
                "'order_amount':654," +
                "'dt':'hgdff'},{'user_id':'123'," +
                "'sku_id':'123'," +
                "'user_gender':'M'," +
                "'user_age':12," +
                "'user_level':1," +
                "'order_price':123.2," +
                "'sku_name':'asdgffadddddddddddddds手机双卡双待模式手机小米华为'," +
                "'sku_tm_id':'234'," +
                "'sku_category1_id':'435'," +
                "'sku_category2_id':'3465'," +
                "'sku_category3_id':'56'," +
                "'sku_category1_name':'dfg'," +
                "'sku_category2_name':'hg'," +
                "'sku_category3_name':'bc'," +
                "'spu_id':'876'," +
                "'sku_num':234," +
                "'order_count':234," +
                "'order_amount':654," +
                "'dt':'hgdff'},{'user_id':'123'," +
                "'sku_id':'123'," +
                "'user_gender':'M'," +
                "'user_age':12," +
                "'user_level':1," +
                "'order_price':123.2," +
                "'sku_name':'asd'," +
                "'sku_tm_id':'234'," +
                "'sku_category1_id':'435'," +
                "'sku_category2_id':'3465'," +
                "'sku_category3_id':'56'," +
                "'sku_category1_name':'dfg'," +
                "'sku_category2_name':'hg'," +
                "'sku_category3_name':'bc'," +
                "'spu_id':'876'," +
                "'sku_num':234," +
                "'order_count':234," +
                "'order_amount':654," +
                "'dt':'hgdff'}," +
                "{'user_id':'123'," +
                "'sku_id':'123'," +
                "'user_gender':'M'," +
                "'user_age':12," +
                "'user_level':1," +
                "'order_price':123.2," +
                "'sku_name':'asd'," +
                "'sku_tm_id':'234'," +
                "'sku_category1_id':'435'," +
                "'sku_category2_id':'3465'," +
                "'sku_category3_id':'56'," +
                "'sku_category1_name':'dfg'," +
                "'sku_category2_name':'hg'," +
                "'sku_category3_name':'bc'," +
                "'spu_id':'876'," +
                "'sku_num':234," +
                "'order_count':234," +
                "'order_amount':654," +
                "'dt':'hgdff'}" +
                "]}}";
    }

    *//*@RequestMapping(value = "getSexData", method = RequestMethod.GET)
    @ResponseBody
    public String getSexData(String time,Integer level,String text){
        //System.out.println(time+level+text);
        *//**//*String json = HttpClientUtil.doGet(sexUrl + "?from=" + s + "&&time=" + time + "&&text=" + text + "&&level=" + l + "&&size=" + size);*//**//*
        return "{'stat':[{'group':[{'name':'20岁以下','value':300},{'name':'20-30岁','value':200},{'name':'30岁以上','value':100}]},{'group':[{'name':'男','value':200},{'name':'女','value':200}]}]}";
    }*//*

    //获取地图数据
    @RequestMapping(value = "getChinaOrderData", method = RequestMethod.GET)
    @ResponseBody
    public String getChinaOrderData(){
        return "";
    }*/

    @Autowired
    private IndexService indexService;

    @RequestMapping(value = "list", method = RequestMethod.GET)
    @ResponseBody
    public Map<String,Object> getList() {
        Map<String,Object> map = new HashMap<>();
        map.put("msg", "ok");
        map.put("data", indexService.findAll());
        return map;
    }
}
