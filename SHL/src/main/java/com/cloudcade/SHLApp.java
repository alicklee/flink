package com.cloudcade;

import com.alibaba.fastjson.JSONObject;
import com.cloudcade.slink.ClickHouseBatchSink;
import com.cloudcade.slink.LoginDataSink;
import com.cloudcade.utils.JsonUtils;
import com.cloudcade.utils.KafkaUtils;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static com.cloudcade.utils.FlinkUtils.createKafkaStream;
import static com.cloudcade.utils.FlinkUtils.env;

public class SHLApp {

    public static void main(String[] args) throws Exception {
        KafkaSource<String> source =  KafkaUtils.getKafkaSource(args);
        DataStream<String> kafkaStream = createKafkaStream(source);
        OutputTag<String> loginOutputTag = new OutputTag<String>("login-output") {};
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("dirty-output") {};
        SingleOutputStreamOperator<String> mainStream = kafkaStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);

                    if (!jsonObject.getString("action").equals("login")) {
                        // Emit to the main output
                        out.collect(value);
                    } else {
                        ctx.output(loginOutputTag, value);
                    }
                } catch (Exception e) {
                    // Handle the exception (e.g., log it or emit to a side output)
                    ctx.output(dirtyOutputTag, value); // Example: Emit to an error side output
                }
            }
        });
        DataStream<String> loginStream = mainStream.getSideOutput(loginOutputTag);
        DataStream<String> dirtyStream = mainStream.getSideOutput(dirtyOutputTag);
        System.out.println("===========start==========");
        loginStream.map(JsonUtils.getMapFunction()).addSink(new LoginDataSink("login_data",args[0]));
        dirtyStream.print();
        mainStream.map(JsonUtils.getMapFunction()).addSink(new ClickHouseBatchSink("bi",args[0]));
        System.out.println("===========end==========");
        env.execute("job");
    }
}
