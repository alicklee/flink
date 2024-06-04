package com.cloudcade.shushu.utils;

import cn.thinkingdata.tga.javasdk.ThinkingDataAnalytics;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ShuShuConsumer {
    private static final String URL =   "http://shangdcsosshushu.leiting.com/";
    //prod
    //private static final String APPID = "256b7aa316aa4ff195c3e91bbf205211";
    //dev
    private static final String APPID = "e7bb93dbd9d648eead4d9a0996ab652e";
    private static ThinkingDataAnalytics ta = null;
    private static final String LOG_DIRECTORY = "./log";

    public ShuShuConsumer() throws URISyntaxException {
         ThinkingDataAnalytics.LoggerConsumer loggerConsumer = new ThinkingDataAnalytics.LoggerConsumer(LOG_DIRECTORY,10);
        ta = new ThinkingDataAnalytics(new ThinkingDataAnalytics.LoggerConsumer(LOG_DIRECTORY));
    }

    public static void push(Map<String,Object> properties,String accountId,String distinctId,String eventName)  {
        //上传事件，包含用户的访客ID与账号ID，请注意不要将访客ID与账号ID写反
        try {
            ta.track(accountId,distinctId,eventName,properties);
            ta.flush();
            System.out.println("properties ===>"+properties);
        } catch (Exception e) {
            //异常处理
            System.out.println("except:"+e);
        }
    }

    public static void setSupperProperties(Map<String,Object> supperProperties){
        try {
            ta.setSuperProperties(supperProperties);
        }catch (Exception e){
            //异常处理
            System.out.println("except:"+e);
        }
    }

    public static void setUserProperties(Map<String,Object> userProperties,String accountId,String distinctId){
        try {
            ta.user_set(accountId,distinctId,userProperties);
        }catch (Exception e){
            //异常处理
            System.out.println("except:"+e);
        }
    }
}

