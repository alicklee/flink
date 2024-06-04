package com.cloudcade.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.util.Properties;

public class FlinkUtils {

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<String> createKafkaStream() throws IOException, InstantiationException, IllegalAccessException {
        // ParameterTool tool = ParameterTool.fromPropertiesFile("config/config.properties");
        //从配置文件获取配置
        //String groupId = tool.get("group.id","test");
        //String servers = tool.getRequired("bootstrap.servers");
        //List<String> topics = Arrays.asList(tool.getRequired("kafka.input.topics") .split(",")).stream().map(s -> (s.trim())).collect(Collectors.toList());
        //String enableCommit = tool.get("enable.auto.commit", "false");
        //String offsetReset = tool.get("auto.offset.reset", "earliest");
        //获取kafka的ssl配置文件
        //String sslKeyStoreLocation = tool.get("ssl.keystore.location");

        //int checkpointInterval = tool.getInt("checkpoint.interval", 5000);
        //String checkpointPath = tool.get("checkpoint.path");

        //设置checkpoint
        env.enableCheckpointing(5000);
        //env.setStateBackend(new );
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(5,TimeUnit.SECONDS)));

        //设置kafka的配置
//        Properties prop = new Properties();
//        prop.setProperty("bootstrap.servers", "192.168.110.241:9092, 192.168.110.242:9092, 192.168.110.243:9092");
//        System.out.println("new ip address");
//        prop.setProperty("group.id", "beta-shl-dau");
//        prop.setProperty("enable.auto.commit", "false");
//        prop.setProperty("auto.offset.reset", "earliest");
        //设置kafka的ssl配置
//        prop.put("security.ssl.enabled",true);
//        prop.put("security.ssl.keystore","data/client.truststore.jks");
//        prop.put("security.ssl.keystore-password","Cade2021");
//        prop.put("security.ssl.key-password","Cade2021");
//        prop.put("security.ssl.truststore","data/server.truststore.jks");
//        prop.put("security.ssl.truststore-password","Cade2021");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.110.241:9092, 192.168.110.242:9092, 192.168.110.243:9092")
                .setTopics("shl-dau-beta")
                .setGroupId("shl-dau-bi-beta")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        //设置kafka的ssl配置
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");






    }
}
