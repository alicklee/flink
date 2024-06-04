package com.cloudcade.slink;

import com.cloudcade.utils.ClickHouseUtils;
import com.cloudcade.utils.JsonUtils;
import com.cloudcade.utils.LocalConfig;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ClickHouseBatchSink extends RichSinkFunction<Map<String, Object>> {
    private ClickHouseStatement statement;
    private ClickHouseConnection connection;
    private  ClickHouseDataSource dataSource;
    private PreparedStatement preparedStatement = null;
    private final String tableName;
    private final String jobEnv;

    private final List<Map<String, Object>> batch = new ArrayList<>();
    private static final int MAX_BATCH_SIZE = 10;
    private static final long BATCH_TIMEOUT = 3000; // 5 seconds

    public ClickHouseBatchSink(String tableName,String jobEnv ) {
        this.tableName = tableName;
        this.jobEnv = jobEnv;
    }

    @Override
    public void open(Configuration parameters) throws Exception{
        String apolloConfigUrl = LocalConfig.getParamValue(this.jobEnv,"apollo.url");


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

        String ckurl = jsonObject.get("ck.url").getAsString();
        String passwd = jsonObject.get("ck.passwd").getAsString();
        String user = jsonObject.get("ck.user").getAsString();
        String db = jsonObject.get("ck.db").getAsString();
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(user);
        properties.setPassword(passwd);
        properties.setDatabase(db);
        dataSource = new ClickHouseDataSource(ckurl, properties);
        connection = dataSource.getConnection();
        statement = connection.createStatement();
    }

    @Override
    public void invoke(Map<String, Object> value, Context context) throws Exception {
        batch.add(value);
        if (batch.size() >= MAX_BATCH_SIZE || context.currentProcessingTime() >= context.currentProcessingTime() + BATCH_TIMEOUT) {
            insertBatch();
        }
    }

    @Override
    public void close() throws Exception {
        if (!batch.isEmpty()) {
            insertBatch();
        }
    }

    private void insertBatch() throws SQLException {
        if (!batch.isEmpty()) {
            try {
                List<Col> cols = new ArrayList<>(200);
                String fullTable = tableName;
                String sqlHead = "INSERT INTO " + fullTable + "(";
                ResultSet rs = statement.executeQuery("DESC " + fullTable);
                StringBuilder insertSqlBuff = new StringBuilder(sqlHead);
                StringBuilder valuesSqlBuff = new StringBuilder(" VALUES (");
                while (rs.next()) {
                    String name = rs.getString("name");
                    String type = rs.getString("type");
                    String defaultExp = rs.getString("default_expression");
//        log.debug("name:{},type:{},defaultExp:{}", name, type, defaultExp);
                    if (!StringUtils.isBlank(defaultExp)) {
                        continue;
                    }
                    cols.add(new Col(name, type));
                    insertSqlBuff.append(name).append(",");
                    valuesSqlBuff.append("?,");
                }
                rs.close();
                statement.close();
                if (cols.size() == 0) {
                    return ;
                }
                insertSqlBuff.deleteCharAt(insertSqlBuff.length() - 1).append(")");
                valuesSqlBuff.deleteCharAt(valuesSqlBuff.length() - 1).append(")");
                insertSqlBuff.append(valuesSqlBuff);
                System.out.println(insertSqlBuff.toString());
                PreparedStatement ps = connection.prepareStatement(insertSqlBuff.toString());
                for (Map<String, Object> data : batch) {

                    if (data.size() == 0) {
                        continue;
                    }
                    for (int i = 0; i < cols.size(); i++) {
                        Col col = cols.get(i);
                        int idx = i + 1;
                        switch (col.type) {
                            case "String": {
                                Object value = data.get(col.name);
                                if (value == null) {
                                    ps.setString(idx, "");
                                } else {
                                    ps.setString(idx, value.toString());
                                }
                            }
                            break;
                            case "Int32": {
                                Object value = data.get(col.name);
                                if (value == null || value.equals("")) {
                                    ps.setInt(idx, 0);
                                } else if (value instanceof Integer) {
                                    ps.setInt(idx, (Integer) value);
                                }
                            }
                            break;
                            case "Int64": {
                                Object value = data.get(col.name);
                                if (value == null || value.equals("")) {
                                    ps.setLong(idx, 0);
                                } else if (value instanceof Long) {
                                    ps.setObject(idx, (Long) value);
                                }
                            }
                            break;
                            case "Float32": {
                                Object value = data.get(col.name);
                                if (value == null) {
                                    ps.setFloat(idx, 0);
                                } else if (value instanceof Float) {
                                    ps.setFloat(idx, (Float) value);
                                }
                            }
                            break;
                            case "Float64": {
                                Object value = data.get(col.name);
                                if (value == null) {
                                    ps.setDouble(idx, 0);
                                } else if (value instanceof Double) {
                                    ps.setDouble(idx, (Double) value);
                                }
                            }
                            break;
                            case  "DateTime('Etc/UTC')":{
                                Object value = data.get(col.name);
                                if (value == null ){
                                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    Date date = dateFormat.parse((String) "0000-00-00 00:00:00");
                                    Timestamp timestamp = new Timestamp(date.getTime());
                                    ps.setTimestamp(idx,timestamp);
                                } else if (value instanceof String){
                                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    if (value.equals("")){
                                        value = "0000-00-00 00:00:00";
                                    }
                                    Date date = dateFormat.parse((String) value);
                                    // 将 Date 对象转换为 java.sql.Timestamp（DateTime 类型的一种 Java 表示）
                                    Timestamp timestamp = new Timestamp(date.getTime());
                                    ps.setTimestamp(idx,timestamp);
                                }
                            }
                            break;
                            case "Nullable(Int32)":
                                Object value = data.get(col.name);
                                if (value == null) {
                                    ps.setObject(idx, null);
                                } else if (value instanceof Integer) {
                                    ps.setInt(idx, (Integer) value);
                                }
                                break;
                            default:
                                System.out.println(col.type);
                        }
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
                ps.clearBatch();
                batch.clear();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public PreparedStatement getInsertSql(List<Map<String, Object>> datas, String tableName, ClickHouseStatement stmt, ClickHouseConnection conn) throws SQLException, ParseException {
        List<Col> cols = new ArrayList<>(200);
        String fullTable = "login_data";
        String sqlHead = "INSERT " + fullTable + "(";
        ResultSet rs = stmt.executeQuery("DESC " + fullTable);
        StringBuilder insertSqlBuff = new StringBuilder(sqlHead);
        StringBuilder valuesSqlBuff = new StringBuilder("VALUES (");
        while (rs.next()) {
            String name = rs.getString("name");
            String type = rs.getString("type");
            String defaultExp = rs.getString("default_expression");
//        log.debug("name:{},type:{},defaultExp:{}", name, type, defaultExp);
            if (!StringUtils.isBlank(defaultExp)) {
                continue;
            }
            cols.add(new Col(name, type));
            insertSqlBuff.append(name).append(",");
            valuesSqlBuff.append("?,");
        }
        rs.close();
        stmt.close();
        if (cols.size() == 0) {
            return null;
        }
        insertSqlBuff.deleteCharAt(insertSqlBuff.length() - 1).append(")");
        valuesSqlBuff.deleteCharAt(valuesSqlBuff.length() - 1).append(")");
        insertSqlBuff.append(valuesSqlBuff);
        PreparedStatement ps = conn.prepareStatement(insertSqlBuff.toString());
        return extracted(datas, cols, ps);
    }

    private static PreparedStatement extracted(List<Map<String, Object>> datas, List<Col> cols, PreparedStatement ps) throws SQLException, ParseException {
        for (Map<String, Object> data : datas) {
            if (data.isEmpty()) {
                continue;
            }
            for (int i = 0; i < cols.size(); i++) {
                Col col = cols.get(i);
                int idx = i + 1;
                switch (col.type) {
                    case "String": {
                        Object value = data.get(col.name);
                        if (value == null) {
                            ps.setString(idx, "");
                        } else {
                            ps.setString(idx, value.toString());
                        }
                    }
                    break;
                    case "Int32": {
                        Object value = data.get(col.name);
                        if (value == null) {
                            ps.setInt(idx, 0);
                        } else if (value instanceof Integer) {
                            ps.setInt(idx, (Integer) value);
                        }
                    }
                    break;
                    case "Int64": {
                        Object value = data.get(col.name);
                        if (value == null) {
                            ps.setLong(idx, 0);
                        } else if (value instanceof Long) {
                            ps.setLong(idx, (Long) value);
                        }
                    }
                    break;
                    case "Float32": {
                        Object value = data.get(col.name);
                        if (value == null) {
                            ps.setFloat(idx, 0);
                        } else if (value instanceof Float) {
                            ps.setFloat(idx, (Float) value);
                        }
                    }
                    break;
                    case "Float64": {
                        Object value = data.get(col.name);
                        if (value == null) {
                            ps.setDouble(idx, 0);
                        } else if (value instanceof Double) {
                            ps.setDouble(idx, (Double) value);
                        }
                    }
                    default:
                        if (col.type.startsWith("DateTime('Etc/UTC')")) {
                            Object value = data.get(col.name);
                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            Date date = dateFormat.parse((String) value);
                            // 将 Date 对象转换为 java.sql.Timestamp（DateTime 类型的一种 Java 表示）
                            Timestamp timestamp = new Timestamp(date.getTime());
                            ps.setTimestamp(idx,timestamp);
                        }
                }
            }
        }
        return ps;
    }




    public static class Col {

        String name;
        String type;

        Col(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }
}
