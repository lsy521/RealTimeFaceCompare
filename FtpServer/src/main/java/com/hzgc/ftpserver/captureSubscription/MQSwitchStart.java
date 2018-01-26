package com.hzgc.ftpserver.captureSubscription;

import com.hzgc.util.common.FileUtil;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 人脸抓拍订阅定时刷新及去除过期数据
 */
public class MQSwitchStart extends CaptureSubscriptionObject {
    private static Logger LOG = Logger.getLogger(MQSwitchStart.class);
    private static MQSwitchClient mqSwitchClient;
    private static String path = MQSwitchImplInit.getPath();
    private CaptureSubscriptionObject object = new CaptureSubscriptionObject();

    static {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(FileUtil.loadResourceFile("rocketmq.properties")));
            String zookeeperAddress = properties.getProperty("zookeeperAddress");
            mqSwitchClient = new MQSwitchClient(10000, zookeeperAddress, path, false);
            mqSwitchClient.createConnection(zookeeperAddress, 10000);
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

    public void start() {
        Thread thread = new Thread() {
            public void run() {
                while (true) {
                    captureSubscription = mqSwitchClient.getData();
                    object.setIpcIdList(captureSubscription);
                    long current = System.currentTimeMillis();
                    LOG.info("777" + "time:" + current + ", captureSubscription:" + captureSubscription + ", ipcIdList:" + object.getIpcIdList());
                    for (String userId : captureSubscription.keySet()) {
                        Map<String, List<String>> map = captureSubscription.get(userId);
                        for (String time : map.keySet()) {
                            if (isInDate(time)) {
                                mqSwitchClient.delete(path + "/" + userId);
                            }
                        }
                    }
                    try {
                        Thread.sleep(1000);
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
