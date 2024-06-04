package com.cloudcade.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class JsonUtils {
    /**
     * map function 转换kafka的json
     *
     * @return
     */
    public static MapFunction<String, Map<String, Object>> getMapFunction() {
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
     * @param jsonObject 传入的json数据
     * @return
     */
    private static Map<String, Object> setUserProperties(JSONObject jsonObject) {
        Map<String, Object> baseProperties = new HashMap<String, Object>();
        Map<String, Object> userDataProperties = new HashMap<String, Object>();
        Map<String, Object> inlineData = new HashMap<String, Object>();
        Map<String, Object> response = new HashMap<String, Object>();
        for (Map.Entry entry : jsonObject.entrySet()) {
            String keyName = entry.getKey().toString();
            if (keyName.contains("#")){
                keyName = keyName.replace("#","");
            }
            if (keyName.equals("userdata")) {
                userDataProperties = setUserDataProperties(JSON.parseObject(entry.getValue().toString()));
                continue;
            }
            if (keyName.equals("inline")){
                inlineData = setInlineDataProperties(JSONObject.parseObject(entry.getValue().toString()));
                continue;
            }

            if (keyName.equals("event_time")) {
                keyName = "event_ts";
            }
            if (keyName.equals("inline")){
                continue;
            }
            baseProperties.put(keyName, entry.getValue());
        }
        response.putAll(userDataProperties);
        response.putAll(baseProperties);
        response.putAll(inlineData);
        return response;
    }

    private static Map<String,Object> setUserDataProperties(JSONObject jsonObject){
        Map<String, Object> userDataProperties = new HashMap<String, Object>();
        String keyPre = "user_";
        for (Map.Entry entry : jsonObject.entrySet()) {
            String keyName = entry.getKey().toString();
            if (keyName.contains("#")){
                keyName = keyName.replace("#","");
            }
            userDataProperties.put(keyPre+keyName, entry.getValue());
        }
        return userDataProperties;
    }

    private static Map<String,Object> setInlineDataProperties(JSONObject jsonObject){
        Map<String, Object> inlineData = new HashMap<String, Object>();
        String keyPre = "inline_";
        for (Map.Entry entry : jsonObject.entrySet()) {
            String keyName = entry.getKey().toString();
            if (keyName.contains("#")){
                keyName = keyName.replace("#","");
            }
            inlineData.put(keyPre+keyName, entry.getValue());
        }
        return inlineData;
    }
}
