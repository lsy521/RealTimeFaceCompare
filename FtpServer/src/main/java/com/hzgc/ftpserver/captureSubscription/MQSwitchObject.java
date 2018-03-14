package com.hzgc.ftpserver.captureSubscription;

import com.hzgc.ftpserver.util.ZookeeperClient;
import com.hzgc.util.common.FileUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * MQ开关对象
 */
public class MQSwitchObject {
    private String path = "/mq_switch";
    private MQSwitchClient mqSwitchClient;
    private boolean show;
    private static MQSwitchObject instance = null;

    /*public MQSwtichObject(){
        Properties properties = new Properties();
        try {
            FileInputStream fis = new FileInputStream(FileUtil.loadResourceFile("rocketmq.properties"));
            properties.load(fis);
            String zookeeperAddress = properties.getProperty("zookeeperAddress");
            mqSwtichClient = new MQSwtichClient(10000,zookeeperAddress,path,false);
            mqSwtichClient.createConnection(zookeeperAddress,10000);
            String data = Arrays.toString(mqSwtichClient.getDate(path));
            show = Boolean.parseBoolean(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/
    public MQSwitchObject() {
        String zookeeperAddress = "172.18.18.100:2181,172.18.18.101:2181,172.18.18.102:2181";
        mqSwitchClient = new MQSwitchClient(10000, zookeeperAddress, path, false);
        mqSwitchClient.createConnection(zookeeperAddress, 10000);
        show = false;
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
