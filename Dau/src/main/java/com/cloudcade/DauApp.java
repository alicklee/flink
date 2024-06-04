package com.cloudcade;

import com.alibaba.fastjson.JSON;
import com.cloudcade.utils.FlinkUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

public class DauApp {
    public static void main(String[] args) throws Exception {
        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream();
        kafkaStream.print();
        System.out.println("=====================");
        DataStreamSink<Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer>> map = kafkaStream.map(new MapFunction<String, Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer>>() {
            @Override
            public Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer> map(String value) throws Exception {

                try {
                    return getTuple14(value);
                }catch (Exception e){
                    return null;
                }
            }
        }).filter(new FilterFunction<Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer> value) throws Exception {
                return value !=null && value.f0 != null;
            }
        }).addSink(JdbcSink.sink(
                "insert into flink_dau values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (pstmt, x)->{
                    pstmt.setString(1, x.f0);
                    pstmt.setString(2, x.f1);
                    pstmt.setString(3, x.f2);
                    pstmt.setString(4, x.f3);
                    pstmt.setString(5, x.f4);
                    pstmt.setString(6, x.f5);
                    pstmt.setString(7, x.f6);
                    pstmt.setString(8, x.f7);
                    pstmt.setString(9, x.f8);
                    pstmt.setString(10, x.f9);
                    pstmt.setString(11, x.f10);
                    pstmt.setInt(12, x.f11);
                    pstmt.setInt(13, x.f12);
                    pstmt.setInt(14,x.f13);
                },
                JdbcExecutionOptions.builder().withBatchSize(3).withBatchIntervalMs(4000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://192.168.110.229:8123/beta")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("default")
                        .withPassword("cade2021")
                        .build()
        ));

        FlinkUtils.env.execute("Beta DAU Job");
    }

    /**
     * 解析接收到的json的值，组装成Tuple14的格式
     * @param value
     * @return
     */
    private static Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer> getTuple14(String value) {
        String userId = JSON.parseObject(value).getString("userId");
        String deviceId = JSON.parseObject(value).getString("deviceId");
        String platform = JSON.parseObject(value).getString("platform");
        String deviceName = JSON.parseObject(value).getString("deviceName");
        String networkType = JSON.parseObject(value).getString("networkType");
        String thirdPartyPlatform = JSON.parseObject(value).getString("thirdPartyPlatform");
        String thirdPartyUserId = JSON.parseObject(value).getString("thirdPartyUserId");
        String thirdPartyToken = JSON.parseObject(value).getString("thirdPartyToken");
        String googleAid = JSON.parseObject(value).getString("googleAid");
        String androidId = JSON.parseObject(value).getString("androidId");
        String idfa = JSON.parseObject(value).getString("idfa");
        Integer registerTime = JSON.parseObject(value).getInteger("registerTime");
        Integer isNew = JSON.parseObject(value).getInteger("isNew");
        Integer eventTime = JSON.parseObject(value).getInteger("eventTime");
        System.out.println(eventTime);
        System.out.println("---------------------");
        Tuple14<String, String, String, String, String, String, String, String, String, String, String, Integer, Integer, Integer> result = new Tuple14<>(userId, deviceId, platform, deviceName, networkType, thirdPartyUserId, thirdPartyPlatform, thirdPartyToken, googleAid, androidId, idfa, registerTime, isNew, eventTime);
        return result;
    }

}

