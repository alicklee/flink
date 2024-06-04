package com.cloudcade;

import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

public class Main {
    public static void main(String[] args) {
        EnvironmentSettings environmentSettings = EnvironmentSettings.inStreamingMode();

        TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);

        //1>
        //
        // {"action":"statue_progress","#distinct_id":"fa6ef49d-8265-4118-b314-98ec4fba7cb3","#event_time":"2023-10-25 08:23:44","#zone_offset":0,"#ip":"222.209.232.42","server_id":"2","server_name":"SHL","channel":"","account":"","#account_id":"1023371","role_name":"ShopKeeper#1023371","level":38,"gold_num":11684005,"diamond_num":25989,"total_charge":0,"ver":"0.0.0","androidid":"","imei":"","oaid":"","mac":"04421AA2842E","termin_info":"ASUS TUF Gaming A15 FA506QM_FA506QM (ASUSTeK COMPUTER INC.)","os_version":"Windows 10  (10.0.19045) 64bit","inline":{"statue_id":"2","level_before":8,"level_after":8,"experience_before":1625,"experience_after":1626},"userdata":{"register_time":"2023-10-17 02:42:52","register_channel":"","register_server_id":"2","register_server_name":"SHL","register_ip":"171.88.21.43","account":"","role_id":"a14094ac-bfea-438a-a215-6de2753648af","role_name":"ShopKeeper#1023371","role_short_id":1023371,"server_serial_number":1,"level":38,"shop_level":4,"channel":"","gender":"M","gold_num":11684005,"diamond_num":25989,"energy_num":125,"craft_slot":9,"trading_slot":0,"bag_num":132,"blueprint_num":129,"attractiveness":2336,"city_id":"08020cd9bba1479c84748692fb6fa39d","explore_progress":304,"first_charge_time":"","total_online_time":148305,"total_charge":0,"last_login_time":"2023-10-25 07:43:50","last_charge_time":"","androidid":"","imei":"","mac":"04421AA2842E","termin_info":"ASUS TUF Gaming A15 FA506QM_FA506QM (ASUSTeK COMPUTER INC.)"}}
        // 建立表
        Table table = tableEnv.from(TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("#account_id",DataTypes.STRING())
                        .column("action",DataTypes.STRING())
                        .column("#event_time",DataTypes.TIMESTAMP())
                                .build())
                        .format("json")
                        .option("topic","shl-bi-test2")
                        .option("properties.bootstrap.servers","192.168.110.241:9092, 192.168.110.242:9092, 192.168.110.243:9092")
                        .option("properties.group_id","shl-bi-test2")
                        .option("scan.startup.mode","earliest-offset")
                        .option("json.fail-on-missing-field","false")
                        .option("json.ignore-parse-errors","true")
                .build());
        Table table1 = table.renameColumns($("#account_id").as("account_id"), $("#event_time").as("event_ts"));
        tableEnv.createTemporaryView("kafka_table",table1);
        tableEnv.executeSql("select action as action ,event_ts as time ,account_id as id from kafka_table").print();
    }
}