package com.cloudcade.utils;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import com.alibaba.fastjson.JSONObject;

public class KafkaUtils {
    public static KafkaSource<String> getKafkaSource(String[] args) throws IOException {
        String apolloConfigUrl = LocalConfig.getParamValue(args[0],"apollo.url");


        URL url = new URL(apolloConfigUrl);
        BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
        String inputLine;
        String content = "";
        while ((inputLine = in.readLine()) != null) {
            content += inputLine;
        }
        in.close();
        System.out.println(content);

        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(content, JsonObject.class);


        String kafkaUrls = jsonObject.get("kafka.servers").getAsString();
        String kafkaTopic = jsonObject.get("kafka.topic").getAsString();
        String kafkaGroupId = jsonObject.get("kafka.groupId").getAsString();


        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaUrls)
                .setTopics(kafkaTopic)
                .setGroupId(kafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        return source;
    }
}
