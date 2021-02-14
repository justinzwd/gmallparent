package com.atguigu.gmall.publishers.gmall_publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.publishers.gmall_publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping
    public String getTotal(@RequestParam("date") String date) {
        List<Map> totalList = new ArrayList<>();
        Map dauMap = new HashMap();

        dauMap.put("id","dau");
        dauMap.put("name","新增日活");

        Integer dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value", dauTotal);

        totalList.add(dauMap);

        Map newMap = new HashMap();

        newMap.put("id","newMid");
        newMap.put("name","新增设备");
        newMap.put("value",233);

        totalList.add(newMap);

        return JSON.toJSONString(totalList);

    }

}
