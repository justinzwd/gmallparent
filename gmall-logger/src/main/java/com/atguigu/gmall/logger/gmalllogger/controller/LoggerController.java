package com.atguigu.gmall.logger.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.constant.GmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController // Controller + ResponseBody
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;
    @RequestMapping(value = "/log", method = RequestMethod.POST)
    //@ResponseBody
    //@PostMapping("/log")
    public String dolog(@RequestParam("log") String logJson) {

        // 1 补时间戳
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts", System.currentTimeMillis());

        // 2 落盘到logfile log4j
        logger.info(jsonObject.toJSONString());

        // 3 发送到kafka
        if ("startup".equals(jsonObject.getString("type"))) {

            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP, jsonObject.toJSONString());
        }
        else {

            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT, jsonObject.toJSONString());
        }

        //System.out.println(logJson);
        return "success";
    }
}
