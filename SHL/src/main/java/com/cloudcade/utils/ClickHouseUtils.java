package com.cloudcade.utils;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ClickHouseUtils {
    public static String insertData(String tableName, Map<String, Object> data) {
        String resultsql = "INSERT INTO ";
        resultsql += tableName + " (";
        String valuesql = "(";
        Set<Map.Entry<String, Object>> sets = data.entrySet();
        for (Map.Entry<String, Object> map : sets) {
            String fieldName = map.getKey();
            Object valuestring = map.getValue();
            if (Objects.equals(valuestring, "")){
                continue;
            }
            resultsql += fieldName + ",";

            if (Objects.equals(valuestring,false)){
                valuestring = 0;
            }

            if (Objects.equals(valuestring,true)){
                valuestring = 1;
            }


            if (valuestring.getClass().getGenericSuperclass() != Number.class) {
                valuesql += "'" + valuestring + "'" + ",";
            } else {
                valuesql += valuestring + ",";
            }
        }
        resultsql = resultsql.substring(0, resultsql.length() - 1) + ")";
        valuesql = valuesql.substring(0, valuesql.length() - 1) + ")";
        resultsql = resultsql + " VALUES " + valuesql;
        System.out.println(resultsql);
        return resultsql;
    }
}
