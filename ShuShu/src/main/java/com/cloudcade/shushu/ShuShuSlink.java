package com.cloudcade.shushu;

import com.cloudcade.shushu.utils.ShuShuConsumer;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.net.URISyntaxException;
import java.util.*;

public class ShuShuSlink extends RichSinkFunction<Map<String, Object>> {
    public static final List<String> userEvent = Arrays.asList("login",
            "logout",
            "create_role",
            "avatar_change",
            "charge",
            "levelup",
            "coin_get",
            "coin_cost",
            "diamond_get",
            "diamond_cost",
            "item_get",
            "item_cost",
            "task_finish",
            "blueprint_upgrade",
            "slot_unlock",
            "slot_upgrade",
            "furniture_buy",
            "furniture_upgrade",
            "shop_upgrade",
            "bag_upgrade",
            "explore",
            "city_create",
            "city_manage"
            );
    public ShuShuSlink() throws URISyntaxException {
    }

    @Override
    public void invoke(Map<String,Object> value, Context context) throws Exception {
        String accountId = value.get("#account_id").toString();
        String distinctId = "";
        if (value.get("#distinct_id") != null) {
             distinctId = value.get("#distinct_id").toString();
        }
        Map<String, Object> superProperties =(Map<String, Object>)value.get("superProperties");
        Map<String, Object> userSetProperties =  (Map<String, Object>) value.get("userSetProperties");
        String eventName = superProperties.get("action").toString();
        ShuShuConsumer.setSupperProperties(superProperties);

        //判断是不是需要更新用户属性的事件
        if (userEvent.contains(eventName)){
            ShuShuConsumer.setUserProperties(userSetProperties,accountId,distinctId);
        }
        value.remove("#account_id");
        value.remove("#distinct_id");
        value.remove("#event_name");
        value.remove("superProperties");
        value.remove("userSetProperties");
        ShuShuConsumer.push(value,accountId,distinctId,eventName);
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
