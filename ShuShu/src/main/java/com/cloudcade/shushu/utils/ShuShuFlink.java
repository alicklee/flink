package com.cloudcade.shushu.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class ShuShuFlink {
    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<String> createKafkaStream() throws IOException, InstantiationException, IllegalAccessException {
        //设置checkpoint
        env.enableCheckpointing(5000);
        //设置kafka的配置
//        Properties prop = new Properties();
//        prop.setProperty("bootstrap.servers", "192.168.110.241:9092, 192.168.110.242:9092, 192.168.110.243:9092");
//        System.out.println("new ip address");
//        prop.setProperty("group.id", "shl-event-dev");
//        prop.setProperty("enable.auto.commit", "false");
//        prop.setProperty("auto.offset.reset", "earliest");
//        return env.addSource(new FlinkKafkaConsumer<>("shl-bi-dev", new SimpleStringSchema(), prop));


        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.110.241:9092,192.168.110.242:9092,192.168.110.243:9092")
                .setTopics("shl-bi-debug")
                .setGroupId("shl-bi-prod")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        //设置kafka的ssl配置
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

    }
}