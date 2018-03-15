package com.hzgc.collect.expand.subscribe;

import java.io.Serializable;

/**
 * MQ开关对象
 */
public class MQSwitchObject implements Serializable {
    private MQSwitchClient mqSwitchClient;
    private boolean mqSwitch;
    private static MQSwitchObject instance = null;

    private MQSwitchObject() {
        mqSwitchClient = new MQSwitchClient(ZookeeperParam.SESSION_TIMEOUT, ZookeeperParam.zookeeperAddress, ZookeeperParam.PATH_SWITCH, ZookeeperParam.WATCHER);
        mqSwitchClient.createConnection(ZookeeperParam.zookeeperAddress, ZookeeperParam.SESSION_TIMEOUT);
        mqSwitch = false;
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

    public boolean isMqSwitch() {
        return mqSwitch;
    }

    public void setMqSwitch(boolean mqSwitch) {
        mqSwitchClient.setData(mqSwitch);
        this.mqSwitch = mqSwitch;
    }
}
