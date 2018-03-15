package com.hzgc.collect.expand.subscribe;

import java.io.Serializable;

/**
 * MQ开关对象
 */
public class MQSwitchObject implements Serializable {
    private MQSwitchClient mqSwitchClient;
    private boolean show;
    private static MQSwitchObject instance = null;

    private MQSwitchObject() {
        mqSwitchClient = new MQSwitchClient(ZookeeperParam.SESSION_TIMEOUT, ZookeeperParam.zookeeperAddress, ZookeeperParam.PATH_SWITCH, ZookeeperParam.WATCHER);
        mqSwitchClient.createConnection(ZookeeperParam.zookeeperAddress, ZookeeperParam.SESSION_TIMEOUT);
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

    public boolean isShow() {
        return show;
    }

    public void setShow(boolean show) {
        mqSwitchClient.setData(show);
        this.show = show;
    }
}
