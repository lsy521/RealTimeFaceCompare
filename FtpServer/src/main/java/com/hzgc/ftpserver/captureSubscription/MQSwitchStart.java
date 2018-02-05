package com.hzgc.ftpserver.captureSubscription;

import com.hzgc.util.common.FileUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * 人脸抓拍订阅定时刷新及去除过期数据
 */
public class MQSwitchStart extends CaptureSubscriptionObject {
    private static Logger LOG = Logger.getLogger(MQSwitchStart.class);
    private MQSwitchClient mqSwitchClient;
    private String path = MQSwitchImplInit.getPath();
    private CaptureSubscriptionObject object = CaptureSubscriptionObject.getInstance();

    public MQSwitchStart() {
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
                Properties properties = new Properties();
                Map<String, Map<String, List<String>>> map_copy;
                List<String> list_copy;
                while (true) {
                    try {
                        properties.load(new FileInputStream(
                                new File(ClassLoader.getSystemResource("rocketmq.properties").getPath())));
                        boolean openShow = Boolean.parseBoolean(properties.getProperty("mqshow"));
                        String ipcIds = properties.getProperty("mqshow_ipcid");
                        if (!openShow) {
                            map_copy = mqSwitchClient.getData();
                            if (!map_copy.equals(captureSubscription)) {
                                captureSubscription = map_copy;
                                object.setIpcIdList(map_copy);
                                LOG.info("OpenShow = false; Update ipcIdList");
                                for (String userId : captureSubscription.keySet()) {
                                    Map<String, List<String>> map = captureSubscription.get(userId);
                                    for (String time : map.keySet()) {
                                        if (isInDate(time)) {
                                            mqSwitchClient.delete(path + "/" + userId);
                                        }
                                    }
                                }
                            }
                            LOG.info("OpenShow = false; ipcIdList:" + object.getIpcIdList());
                        } else {
                            list_copy = Arrays.asList(ipcIds.split(","));
                            if (!list_copy.equals(object.getIpcIdList())) {
                                object.setIpcIdList(list_copy);
                                LOG.info("OpenShow = true; Update ipcIdList");
                            }
                            LOG.info("OpenShow = true; ipcIdList:" + object.getIpcIdList());
                        }
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            LOG.error("MQ Switch thread error!");
                            e.printStackTrace();
                        }
                    } catch (IOException e) {
                        LOG.error("rocketmq.properties properties error!");
                        e.printStackTrace();
                    }
                }
            }
        };
        thread.start();
        LOG.info("MQ Switch started!");
    }
}
