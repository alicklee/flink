package com.cloudcade.slink;

import com.cloudcade.utils.ClickHouseUtils;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;

public class ActionSlink extends RichSinkFunction<Map<String, Object>> {
    private Connection conn;
    private PreparedStatement insertStmt;
    private ClickHouseProperties properties;
    private ClickHouseDataSource dataSource;
    private String TableName;

    @Override
    public void open(Configuration parameters) throws Exception{
        Configuration flinkConfig = new Configuration();

// 从Flink的配置中读取Apollo的地址和命名空间
        String apolloConfigServiceUrl = flinkConfig.getString("apollo.configServiceUrl", "http://apollo.cade.com/");
        String apolloNamespace = flinkConfig.getString("apollo.namespace", "shl-shushu-dev");

// 初始化Apollo配置
        Config config = ConfigService.getConfig(apolloNamespace);
        String jobName = config.getProperty("jobName", "");
        System.out.println(jobName);
        properties = new ClickHouseProperties();
        String url = config.getProperty("ck.url","default");
        String passwd = config.getProperty("ck.passwd","default");
        String user = config.getProperty("ck.user","default");
        String db = config.getProperty("ck.db","default");
        TableName = config.getProperty("ck.table","default");
        properties.setUser(user);
        properties.setPassword(passwd);
        properties.setDatabase(db);
        dataSource = new ClickHouseDataSource(url,properties);
    }

    @Override
    public void invoke(Map<String, Object> value, Context context) throws Exception {
        String sql = ClickHouseUtils.insertData(TableName,value);
        dataSource.getConnection().createStatement().execute(sql);
        super.invoke(value, context);
    }
}
