package com.cloudcade.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LocalConfig {
    public static String getParamValue(String env,String key) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String filename = "application.properties";
        if (!env.isEmpty()) {
            filename = "application-" + env +".properties";
        }

        try (InputStream input = classLoader.getResourceAsStream("application-" + env +".properties")) {
            Properties properties = new Properties();
            if (input != null) {
                properties.load(input);

                // 从properties对象中读取配置值
                String paramValue = properties.getProperty(key, "default_value");
                return paramValue;
            } else {
                System.out.println("无法找到配置文件 application.properties");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 如果出现异常或无法找到配置文件，可以返回默认值
        return "default_value";
    }
}
