package com.atguigu.gmall.publishers.gmall_publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.publishers.gmall_publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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

    @GetMapping("realtime-hour")
    public String getHourTotal(@RequestParam("id") String id ,@RequestParam("date") String today){
        if("dau".equals(id)){
            //今天
            Map dauHourTDMap = publisherService.getDauHourMap(today);
            //求昨天分时明细
            String yesterday = getYesterday(today);
            Map dauHourYDMap = publisherService.getDauHourMap(yesterday);

            Map hourMap=new HashMap();
            hourMap.put("today",dauHourTDMap);
            hourMap.put("yesterday",dauHourYDMap);
            return  JSON.toJSONString(hourMap);
        }
        //else if("orderAmount".equals(id)){
        //    //今天
        //    Map orderAmountHourTDMap = publisherService.getOrderAmontHourMap(today);
        //    //求昨天分时明细
        //    String yesterday = getYesterday(today);
        //    Map orderAmountHourYDMap = publisherService.getOrderAmontHourMap(yesterday);
        //
        //    Map hourMap=new HashMap();
        //    hourMap.put("today",orderAmountHourTDMap);
        //    hourMap.put("yesterday",orderAmountHourYDMap);
        //    return  JSON.toJSONString(hourMap);
        //
        //
        //}


        return  null;
    }

    private  String getYesterday(String today){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday="";
        try {
            Date todayDate = simpleDateFormat.parse(today);
            Date yesterdayDate = DateUtils.addDays(todayDate, -1);
            yesterday = simpleDateFormat.format(yesterdayDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yesterday;
    }


}
