package com.cloudcade;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;

public class CBSlink extends RichSinkFunction<Map<String, Object>>  {
    private  Connection conn;
    private PreparedStatement insertStmt;
    private ClickHouseProperties properties;
    private ClickHouseDataSource dataSource;

    @Override
    public void open(Configuration parameters) throws Exception{
        properties = new ClickHouseProperties();
        String url = "jdbc:clickhouse://192.168.110.229:8123";
        properties.setUser("default");
        properties.setPassword("cade2021");
        properties.setDatabase("beta");
        dataSource = new ClickHouseDataSource(url,properties);
    }

    @Override
    public void invoke(Map<String, Object> value, Context context) throws Exception {
        String sql = ClickHouseUtils.insertData("cb_bi",value);
        dataSource.getConnection().createStatement().execute(sql);
        super.invoke(value, context);
    }

    @Override
    public void writeWatermark(Watermark watermark) throws Exception {
        super.writeWatermark(watermark);
    }
    @Override
    public void finish() throws Exception {
        super.finish();
    }
}
