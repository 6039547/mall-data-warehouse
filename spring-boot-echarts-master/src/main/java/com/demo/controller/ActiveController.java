package com.demo.controller;

import com.demo.model.*;
import com.demo.service.ActiveDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Controller
public class ActiveController {

    @Autowired
    private ActiveDataService activeDataService;


    /*
    *  获取数据头信息
    * */
    @RequestMapping("/queryActiveTotalData")
    @ResponseBody
    public List<ActiveTotalData> queryActiveTotalData(String thisEntryDate){
        return activeDataService.queryActiveTotal(thisEntryDate);
    }

    /*
    *  获取折线图数据
    * */
    @RequestMapping("/queryActiveData")
    @ResponseBody
    public List<ActiveData> queryActiveData(String tag,String thisEntryDate){
        return activeDataService.queryActiveData(tag,thisEntryDate);
    }

    @RequestMapping("/queryRetainData")
    @ResponseBody
    public List<RetainData> queryRetainData(){
        return activeDataService.queryRetainData();
    }

    @RequestMapping("/queryGMVOrder")
    @ResponseBody
    public List<GMVOrder> queryGMVOrder(){
        return activeDataService.queryGMVOrder();
    }
    @RequestMapping("/queryMapData")
    @ResponseBody
    public List<MapData> queryMapData(){
        return activeDataService.queryMapData();
    }

    @RequestMapping(value = "queryConvertData", method = RequestMethod.GET)
    @ResponseBody
    public List<HashMap<String, String>> getList(String tag) {
        ConvertData convertData = activeDataService.queryConvertData(tag);
        ArrayList<HashMap<String, String>> list = new ArrayList<HashMap<String, String>>();


        HashMap<String, String> convert1 = new HashMap<>();
        convert1.put("name","订单");
        convert1.put("value",String.valueOf(convertData.getVisitor2order_convert_ratio()*100));

        HashMap<String, String> convert2 = new HashMap<>();
        convert2.put("name","支付");
        convert2.put("value",String.valueOf(convertData.getOrder2payment_convert_ratio()*100));

        /*HashMap<String, Double> convert5 = new HashMap<>();
        double visit2Pay_convert_ratio = convertData.getVisit2Pay_convert_ratio();
        convert5.put("全流程",visit2Pay_convert_ratio*100);*/
        list.add(convert1);
        list.add(convert2);

        return list;
    }

}
