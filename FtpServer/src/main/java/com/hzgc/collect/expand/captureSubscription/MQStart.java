package com.hzgc.collect.expand.captureSubscription;

import com.hzgc.util.common.FileUtil;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * 人脸抓拍订阅功能及人脸抓拍演示功能定时刷新及去除过期数据
 */
public class MQStart extends CaptureSubscriptionObject {
    private static Logger LOG = Logger.getLogger(MQStart.class);
    private MQSubscriptionClient mqSubscriptionClient;
    private String mqSubscription_path = MQSubscriptionImpl.getPath();
    private MQShowClient mqShowClient;
    private String mqShow_path = MQShowImpl.getPath();
    private CaptureSubscriptionObject object = CaptureSubscriptionObject.getInstance();

    public MQStart() {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(FileUtil.loadResourceFile("rocketmq.properties")));
            String zookeeperAddress = properties.getProperty("zookeeperAddress");
            mqSubscriptionClient = new MQSubscriptionClient(10000, zookeeperAddress, mqSubscription_path, false);
            mqSubscriptionClient.createConnection(zookeeperAddress, 10000);
            mqShowClient = new MQShowClient(10000, zookeeperAddress, mqShow_path, false);
            mqShowClient.createConnection(zookeeperAddress, 10000);
        } catch (IOException e) {
            LOG.error("zookeeperAddress no found in the \"rocketmq.properties\"");
            e.printStackTrace();
        }
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
                    if (!isClose()){
                        MQSwitchObject.getInstance().setShow(true);
                        if (!isShow()) {
                            captureSubscription = mqSubscriptionClient.getData();
                            object.setIpcIdList(captureSubscription);
                            for (String userId : captureSubscription.keySet()) {
                                Map<String, List<String>> map = captureSubscription.get(userId);
                                for (String time : map.keySet()) {
                                    if (isInDate(time)) {
                                        mqSubscriptionClient.delete(mqSubscription_path + "/" + userId);
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
                                        mqShowClient.delete(mqShow_path + "/" + userId);
                                    }
                                }
                            }
                            LOG.info("OpenShow = true, ipcIdList:" + object.getIpcIdList());
                        }
                    }else if (isClose()) {
                        MQSwitchObject.getInstance().setShow(false);
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
