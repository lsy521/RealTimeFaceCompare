package com.hzgc.collect.expand.subscribe;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;

/**
 * 人脸抓拍订阅功能及人脸抓拍演示功能定时刷新及去除过期数据
 */
public class MQThread extends SubscriptionObject implements Serializable {
    private static Logger LOG = Logger.getLogger(MQThread.class);
    private MQSubscriptionClient mqSubscriptionClient;
    private MQShowClient mqShowClient;
    private SubscriptionObject object = SubscriptionObject.getInstance();

    public MQThread() {
        mqSubscriptionClient = new MQSubscriptionClient(ZookeeperParam.SESSION_TIMEOUT, ZookeeperParam.zookeeperAddress, ZookeeperParam.PATH_SUBSCRIBE, ZookeeperParam.WATCHER);
        mqSubscriptionClient.createConnection(ZookeeperParam.zookeeperAddress, ZookeeperParam.SESSION_TIMEOUT);
        mqShowClient = new MQShowClient(ZookeeperParam.SESSION_TIMEOUT, ZookeeperParam.zookeeperAddress, ZookeeperParam.PATH_MQSHOW, ZookeeperParam.WATCHER);
        mqShowClient.createConnection(ZookeeperParam.zookeeperAddress, ZookeeperParam.SESSION_TIMEOUT);
    }

    private boolean isInDate(String time) {
        Calendar now = Calendar.getInstance();
        now.add(Calendar.MONTH, -6);
        long endTime = now.getTimeInMillis();
        long startTime = Long.parseLong(time);
        return startTime <= endTime;
    }

    /**
     * 定位功能
     *
     * @return true表示当前属于演示功能，false表示当前属于订阅功能
     */
    private boolean isShow() {
        List<String> children = mqShowClient.getChildren();
        return !children.isEmpty();
    }

    /**
     * 判断订阅和演示功能是否打开
     *
     * @return true表示演示和订阅功能两者都未打开，false表示演示和订阅功能两者有一个打开
     */
    private boolean isClose() {
        List<String> mqShowChildren = mqShowClient.getChildren();
        List<String> mqSubscriptionChildren = mqSubscriptionClient.getChildren();
        return mqShowChildren.isEmpty() && mqSubscriptionChildren.isEmpty();
    }

    public void start() {
        Thread thread = new Thread() {
            public void run() {
                while (true) {
                    if (!isClose()) {
                        MQSwitchObject.getInstance().setMqSwitch(true);
                        if (!isShow()) {
                            captureSubscription = mqSubscriptionClient.getData();
                            object.setIpcIdList(captureSubscription);
                            for (String userId : captureSubscription.keySet()) {
                                Map<String, List<String>> map = captureSubscription.get(userId);
                                for (String time : map.keySet()) {
                                    if (isInDate(time)) {
                                        mqSubscriptionClient.delete(ZookeeperParam.PATH_SUBSCRIBE + "/" + userId);
                                    }
                                }
                            }
                            LOG.info("OpenShow = false, ipcIdList:" + object.getIpcIdList());
                        } else {
                            captureSubscription = mqShowClient.getData();
                            object.setIpcIdList(captureSubscription);
                            for (String userId : captureSubscription.keySet()) {
                                Map<String, List<String>> map = captureSubscription.get(userId);
                                for (String time : map.keySet()) {
                                    if (isInDate(time)) {
                                        mqShowClient.delete(ZookeeperParam.PATH_MQSHOW + "/" + userId);
                                    }
                                }
                            }
                            LOG.info("OpenShow = true, ipcIdList:" + object.getIpcIdList());
                        }
                    } else if (isClose()) {
                        MQSwitchObject.getInstance().setMqSwitch(false);
                        LOG.info("Close MQ switch");
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOG.error("MQstart thread error!");
                        e.printStackTrace();
                    }
                }
            }
        };
        thread.start();
        LOG.info("The face snapshot subscription function starts successfully!");
    }
}
