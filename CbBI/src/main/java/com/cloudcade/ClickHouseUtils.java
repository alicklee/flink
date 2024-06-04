package com.cloudcade;

import java.util.*;

/**
 * clickhouse 通过map转成插入sql的工具类
 */
public class ClickHouseUtils {
    public static String insertData(String tableName, Map<String, Object> data) {
        String resultsql = "INSERT INTO ";
        resultsql += tableName + " (";
        String valuesql = "(";
        Set<Map.Entry<String, Object>> sets = data.entrySet();
        for (Map.Entry<String, Object> map : sets) {
            String fieldName = map.getKey();
            Object valuestring = map.getValue();
            resultsql += fieldName + ",";

            if (Objects.equals(fieldName, "pve_combat_is_win")){
                System.out.println(valuestring.getClass().getGenericSuperclass());
                System.out.println(
                        "==========="
                );
            }

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

    public static void main(String[] args) {
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("id", 111);
        data.put("name", "xiaobai");
        data.put("create_date", "2018-09-07");
        insertData("youfantest", data);
    }
}