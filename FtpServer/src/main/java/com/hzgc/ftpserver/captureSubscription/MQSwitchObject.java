package com.hzgc.ftpserver.captureSubscription;

import com.hzgc.util.common.FileUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * MQ开关对象
 */
public class MQSwitchObject {
    private MQSwitchClient mqSwitchClient;
    private boolean show;
    private static MQSwitchObject instance = null;

    public MQSwitchObject(){
        Properties properties = new Properties();
        try {
            FileInputStream fis = new FileInputStream(FileUtil.loadResourceFile("rocketmq.properties"));
            properties.load(fis);
            String zookeeperAddress = properties.getProperty("zookeeperAddress");
            String path = MQSwitchClient.getPath();
            mqSwitchClient = new MQSwitchClient(10000,zookeeperAddress, path,false);
            mqSwitchClient.createConnection(zookeeperAddress,10000);
            show = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static MQSwitchObject getInstance() {
        if (instance == null) {
            synchronized (MQSwitchObject.class) {
                if (instance == null) {
                    instance = new MQSwitchObject();
                }
            }
        }
        return instance;
    }

    public MQSwitchClient getMqSwitchClient() {
        return mqSwitchClient;
    }

    public void setMqSwitchClient(MQSwitchClient mqSwitchClient) {
        this.mqSwitchClient = mqSwitchClient;
    }

    public boolean isShow() {
        return show;
    }

    public void setShow(boolean show) {
        mqSwitchClient.setData(show);
        this.show = show;
    }
}
