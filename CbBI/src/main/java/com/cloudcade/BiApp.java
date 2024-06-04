package com.cloudcade;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.HashMap;
import java.util.Map;

import static com.cloudcade.utils.FlinkUtils.createKafkaStream;
import static com.cloudcade.utils.FlinkUtils.env;

public class BiApp {

    public static void main(String[] args) throws Exception {
        final DataStream<String> kafkaStream = createKafkaStream();
        //kafkaStream.print();
        System.out.println("===========start==========");
        kafkaStream.map(getMapFunction()).addSink(new CBSlink());
        System.out.println("===========end==========");
        env.execute("job");
    }

    /**
     * map function 转换kafka的json
     *
     * @return
     */
    private static MapFunction<String, Map<String, Object>> getMapFunction() {
        return new MapFunction<String, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(String s) throws Exception {
                Map<String, Object> properties = new HashMap<String, Object>();
                JSONObject jsonObject = JSON.parseObject(s);

                return setUserProperties(jsonObject);
            }
        };

    }

    /**
     * 转换成数据库格式的map
     *
     * @param jsonObject
     * @return
     */
    private static Map<String, Object> setUserProperties(JSONObject jsonObject) {
        Map<String, Object> userSetProperties = new HashMap<String, Object>();
        for (Map.Entry entry : jsonObject.entrySet()) {

            userSetProperties.put(entry.getKey().toString(), entry.getValue());
        }
        return userSetProperties;
    }
}