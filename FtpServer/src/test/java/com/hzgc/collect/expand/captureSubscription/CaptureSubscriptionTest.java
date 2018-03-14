package com.hzgc.collect.expand.captureSubscription;

import java.util.ArrayList;
import java.util.List;

public class CaptureSubscriptionTest {
    public static void main(String[] args) {

        MQSubscriptionImpl mqSubscription = new MQSubscriptionImpl();

        /*String userId1 = "user1";
        long time1 = System.currentTimeMillis();
        List<String> user1 = new ArrayList<>();
        user1.add("2L04129PAU01933");
        user1.add("3B0383FPAG00883");
        user1.add("3K01E84PAU00083");
        mqSubscription.openMQReception(userId1,time1,user1);*/

        MQShowImpl mqShow = new MQShowImpl();

       /* long time2 = System.currentTimeMillis();//2018-01-19 11:37:36
        List<String> user2 = new ArrayList<>();
        user2.add("xiao");
        user2.add("qi");
        mqShow.openMQShow("xiaoqi",time2,user2);*/

        //mqShow.closeMQShow("xiaoqi");


        /*String userId2 = "user2";
        long time2 = System.currentTimeMillis();//2018-01-19 11:37:36
        List<String> user2 = new ArrayList<>();
        user2.add("xiao");
        user2.add("qi");
        mqSubscription.openMQReception(userId2,time2,user2);*/

        /*List<String> user3 = new ArrayList<>();
        user3.add("55");
        user3.add("xiaoqi");
        mqSwitch.openShow(user3);*/


        //mqSubscription.closeMQReception("user1");
    }
}
