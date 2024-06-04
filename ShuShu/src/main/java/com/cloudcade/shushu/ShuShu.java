package com.cloudcade.shushu;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cloudcade.shushu.utils.ShuShuConsumer;
import com.cloudcade.shushu.utils.ShuShuFlink;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ShuShu {
   public static ShuShuConsumer ShuShuConsumer;

    static {
        try {
            ShuShuConsumer = new ShuShuConsumer();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception{
        final DataStream<String> kafkaStream = ShuShuFlink.createKafkaStream();
        System.out.println("=====================");
        kafkaStream.print();
        kafkaStream.map(getMapFunction()).addSink(new ShuShuSlink());
        ShuShuFlink.env.execute("ThinkData BI Job");
    }

    /**
     * kafka数据格式转换
     * @return
     */
    private static MapFunction<String, Map<String, Object>> getMapFunction() {
        return new MapFunction<String, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(String s) throws Exception {
                Map<String, Object> properties = new HashMap<String, Object>();
                JSONObject jsonObject = JSON.parseObject(s);

                //设置带#的预设属性
                properties = setPublicProperties(properties,jsonObject);

                Map<String, Object> superProperties = setSuperProperties(jsonObject);

                //设置公共属性
                properties.put("superProperties",superProperties);

                //设置用户属性
                    Map<String, Object> userSetProperties = setUserProperties(jsonObject.getJSONObject("userdata"));
                    //同步用户属性的time属性
                    userSetProperties.put("#time",jsonObject.getDate("#event_time"));
                    properties.put("userSetProperties",userSetProperties);

                for (Entry entry : jsonObject.getJSONObject("inline").entrySet()) {
                    properties.put(entry.getKey().toString(), entry.getValue());
                }

                return properties;
            }
        };
    }

    /**
     * 设置带#的预设属性
     * @param jsonObject
     * @return
     */
    private static Map<String, Object> setPublicProperties( Map<String, Object> properties,JSONObject jsonObject) {
        JSONObject jsonObjectTmp = jsonObject.clone();
        jsonObjectTmp.remove("inline");
        jsonObjectTmp.remove("userdata");
        properties.put("#time",jsonObject.getDate("#event_time"));
        for (Entry entry : jsonObjectTmp.entrySet()) {
            if (entry.getKey().toString().contains("#")){
                properties.put(entry.getKey().toString(),entry.getValue());
            }
        }
        return properties;
    }

    /**
     * 添加公共属性
     * @param jsonObject
     * @return
     */
    private static Map<String, Object> setSuperProperties(JSONObject jsonObject) {
        Map<String,Object> BaseProperties = new HashMap<String,Object>();
        JSONObject tmp = jsonObject.clone();
        tmp.remove("inline");
        tmp.remove("userdata");
        for (Entry entry : tmp.entrySet()) {
            if (entry.getKey().toString().contains("#")){
                continue;
            }
            BaseProperties.put(entry.getKey().toString(), entry.getValue());
        }
        return BaseProperties;
    }

    /**
     * 添加用户属性
     * @param jsonObject
     * @return
     */
    private static Map<String, Object> setUserProperties(JSONObject jsonObject) {
        Map<String, Object> userSetProperties = new HashMap<String, Object>();
        for (Entry entry : jsonObject.entrySet()) {
            userSetProperties.put(entry.getKey().toString(), entry.getValue());
        }
        return userSetProperties;
    }

}
