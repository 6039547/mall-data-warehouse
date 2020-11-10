package com.RuHuTian.udtf;


import jodd.util.StringUtil;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

/**
 * @program: shop
 * @description :dwd层事件日志自定义udtf, 用来展开事件数组。
 * @author: RuHuTian
 * @create: 2020-09-28 20:00
 **/
public class EventJsonUDTF extends GenericUDTF {

    @Override
    public StandardStructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        ArrayList<String> nameList = new ArrayList<String>();
        ArrayList<ObjectInspector> typeList = new ArrayList<ObjectInspector>();
        nameList.add("even_name");
        typeList.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        nameList.add("even_json");
        typeList.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(nameList, typeList);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        try {
            String Input = objects[0].toString();
            if (StringUtil.isBlank(Input)) {
                return;
            }
            JSONArray eventJsonArray = new JSONArray(Input);
            for (int i = 0; i < eventJsonArray.length(); i++) {
                String[] result = new String[2];
                String eventName = eventJsonArray.getJSONObject(i).getString("en");
                String eventJson = eventJsonArray.getString(i);
                result[0] = eventName;
                result[1] = eventJson;
                forward(result);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws HiveException {

    }

    public static void main(String[] args) throws HiveException {
        EventJsonUDTF eventJsonUDTF = new EventJsonUDTF();
        String opt = "[{\"ett\":\"1600963966267\",\"en\":\"display\",\"kv\":{\"goodsid\":\"0\",\"action\":\"1\",\"extend1\":\"2\",\"place\":\"2\",\"category\":\"15\"}},{\"ett\":\"1600947029464\",\"en\":\"ad\",\"kv\":{\"entry\":\"1\",\"show_style\":\"1\",\"action\":\"3\",\"detail\":\"\",\"source\":\"2\",\"behavior\":\"1\",\"content\":\"1\",\"newstype\":\"0\"}},{\"ett\":\"1600941132444\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1600988448167\",\"action\":\"1\",\"type\":\"4\",\"content\":\"\"}},{\"ett\":\"1600947662325\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"1\"}}]";
        Object[] objects = new Object[]{opt};
        eventJsonUDTF.process(objects);
    }
}
