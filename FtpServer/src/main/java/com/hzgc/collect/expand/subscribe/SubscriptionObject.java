package com.hzgc.collect.expand.subscribe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 抓拍订阅对象
 */
public class SubscriptionObject implements Serializable {

    //zookeeper中保存的抓拍订阅设备信息
    Map<String, Map<String, List<String>>> captureSubscription;
    //抓拍订阅设备列表
    private volatile List<String> ipcIdList;

    private static SubscriptionObject instance = null;

    SubscriptionObject() {
    }

    public static SubscriptionObject getInstance() {
        if (instance == null) {
            synchronized (SubscriptionObject.class) {
                if (instance == null) {
                    instance = new SubscriptionObject();
                }
            }
        }
        return instance;
    }

    public List<String> getIpcIdList() {
        return ipcIdList;
    }

    void setIpcIdList(List<String> ipcIdList) {
        this.ipcIdList = ipcIdList;
    }

    void setIpcIdList(Map<String, Map<String, List<String>>> map) {
        List<String> ipcIdList = new ArrayList<>();
        if (!map.isEmpty()) {
            for (String userId : map.keySet()) {
                if (userId != null && !userId.equals("")) {
                    Map<String, List<String>> map1 = map.get(userId);
                    if (!map1.isEmpty()) {
                        for (String time : map1.keySet()) {
                            if (time != null && !time.equals("")) {
                                ipcIdList.addAll(map1.get(time));
                            }
                        }
                    }
                }
            }
        }
        this.ipcIdList = ipcIdList;
    }
}
