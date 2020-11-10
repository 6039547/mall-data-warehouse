package com.RuHuTian.udf;

import jodd.util.StringUtil;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @program: hivefunction
 * @description: dwd层事件日志自定义udf, 用来处理日志导入和展开共用字段
 * @author: RuHuTian
 * @create: 2020-09-28 18:23
 **/
public class BaseFieldUDF extends UDF {
    public static void main(String[] args) throws JSONException {
        String line = "1601009689708|{\"cm\":{\"ln\":\"-65.9\",\"sv\":\"V2.4.5\",\"os\":\"8.1.7\",\"g\":\"9U1974C4@gmail.com\",\"mid\":\"381\",\"nw\":\"4G\",\"l\":\"pt\",\"vc\":\"8\",\"hw\":\"1080*1920\",\"ar\":\"MX\",\"uid\":\"381\",\"t\":\"1600922855802\",\"la\":\"-8.6\",\"md\":\"HTC-16\",\"vn\":\"1.2.7\",\"ba\":\"HTC\",\"sr\":\"J\"},\"ap\":\"app\"}";
        String jsonKeys = "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t  ";
        String res = new BaseFieldUDF().evaluate(line, jsonKeys);
        int leng = res.split("\t").length;
        System.out.println(res);
        System.out.println(leng);
    }

    /**
     * @param line     当前行数据（实际只有一个字段，该字段存储一个json字符串）
     * @param jsonKeys 公用字段的key数组
     * @Description: 自定义udf业务逻辑处理，抽取公共字段、事件名、事件json、时间作为列值
     * @return: java.lang.String
     * @Author: RuHuTian
     * @Date: 2020/09/28 18:54
     */
    public String evaluate(String line, String jsonKeys) {
        String[] keys = jsonKeys.split(",");
        String[] logContents = line.split("\\|");
        StringBuffer sb = new StringBuffer();
        if (logContents.length != 2 || StringUtil.isBlank(logContents[1])) {
            return "";
        }

        JSONObject json = null;
        try {
            json = new JSONObject(logContents[1]);
            JSONObject jsonCM = json.getJSONObject("cm");
            for (int i = 0; i < keys.length; i++) {
                String filedName = keys[i].trim();
                if (jsonCM.has(filedName)) {
                    sb.append(jsonCM.getString(filedName)).append("\t");
                } else {
                    sb.append("\t");
                }
            }
            sb.append(json.getString("et")).append("\t");
        } catch (JSONException e) {
            return "";
        }
        sb.append(logContents[0]).append("\t");
        return sb.toString();
    }
}
