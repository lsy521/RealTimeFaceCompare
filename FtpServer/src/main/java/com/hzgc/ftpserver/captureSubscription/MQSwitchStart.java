package com.hzgc.ftpserver.captureSubscription;

import com.hzgc.util.common.FileUtil;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.util.PropertiesUtil;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

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
    private CaptureSubscriptionObject object = new CaptureSubscriptionObject();
    //是否打开人脸抓拍订阅演示功能（false：不打开；true：打开此功能）
   /* private boolean openShow = false;
    private String ipcIds = null;*/

    public MQSwitchStart(){
        /*String proPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        LOG.info("---------path : " +proPath);*/
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(FileUtil.loadResourceFile("rocketmq.properties")));
            String zookeeperAddress = properties.getProperty("zookeeperAddress");
            /*openShow = Boolean.parseBoolean(properties.getProperty("mqshow"));
            ipcIds = properties.getProperty("mqshow_ipcid");*/
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
                    Properties properties = new Properties();
                    try {
                        properties.load(new FileInputStream(FileUtil.loadResourceFile("rocketmq.properties")));
                        boolean openShow = Boolean.parseBoolean(properties.getProperty("mqshow"));
                        String ipcIds = properties.getProperty("mqshow_ipcid");
                        LOG.info("mqshow : " + openShow);
                        if (!openShow) {
                            captureSubscription = mqSwitchClient.getData();
                            object.setIpcIdList(captureSubscription);
                            long current = System.currentTimeMillis();
                            LOG.info("time:" + current + ", captureSubscription:" + captureSubscription + ", ipcIdList:" + object.getIpcIdList());
                            for (String userId : captureSubscription.keySet()) {
                                Map<String, List<String>> map = captureSubscription.get(userId);
                                for (String time : map.keySet()) {
                                    if (isInDate(time)) {
                                        mqSwitchClient.delete(path + "/" + userId);
                                    }
                                }
                            }
                        }else {
                            //String ipcIds = properties.getProperty("mqshow_ipcid");
                            List<String> ipcIdList = Arrays.asList(ipcIds.split(","));
                            object.setIpcIdList(ipcIdList);
                            LOG.info("captureSubscription:" + captureSubscription + ", ipcIdList:" + object.getIpcIdList());
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
