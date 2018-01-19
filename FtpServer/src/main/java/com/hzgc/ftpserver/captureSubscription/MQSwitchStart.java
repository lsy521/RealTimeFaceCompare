package com.hzgc.ftpserver.captureSubscription;

import com.hzgc.ftpserver.util.ZookeeperClient;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * 人脸抓拍订阅定时刷新及去除过期数据
 */
public class MQSwitchStart extends CaptureSubscriptionObject{
    private static Logger LOG = Logger.getLogger(MQSwitchStart.class);
    private ZookeeperClient zookeeperClient = MQSwitchInit.zookeeperClient;
    private String path = MQSwitchInit.path;
    private CaptureSubscriptionObject object = new CaptureSubscriptionObject();

    public void start() {
        Thread thread = new Thread() {
            public void run() {
                while (true) {
                    captureSubscription = zookeeperClient.getMQData();
                    object.setIpcIdList(captureSubscription);
                    long current = System.currentTimeMillis();
                    for (String userId : captureSubscription.keySet()) {
                        Map<String, List<String>> map = captureSubscription.get(userId);
                        for (String time : map.keySet()) {
                            long failureTime = Long.parseLong(time) + 60 * 60 * 24 * 180;
                            if (failureTime >= current) {
                                zookeeperClient.delete(path + "/" + userId);
                            }
                        }
                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        LOG.error("MQ Switch thread error!");
                        e.printStackTrace();
                    }
                }
            }
        };
        thread.start();
        LOG.info("MQ Switch started!");
    }
}
