package com.hzgc.ftpserver.captureSubscription;

import com.hzgc.ftpserver.util.ZookeeperClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;


/**
 * 控制是否打开订阅及演示功能的总开关
 */
public class MQSwitchClient extends ZookeeperClient {
    private static Logger LOG = Logger.getLogger(MQSwitchClient.class);
    private String path = "/mq_swtich";

    public MQSwitchClient(int session_timeout, String zookeeperAddress, String path, boolean watcher) {
        super(session_timeout, zookeeperAddress, path, watcher);
    }

    /**
     * 创建MQ开关节点
     */
    public void createMQSwitchZnode() {
        super.create(path, String.valueOf(false).getBytes());
    }

    /**
     * 获取单个MQ子节点数据(长期调用，故不自动创建连接，不关闭连接)
     */
    public byte[] getDate() {
        byte[] bytes = null;
        try {
            Stat stat = zooKeeper.exists(path, watcher);
            bytes = zooKeeper.getData(path, watcher, stat);
        } catch (KeeperException | InterruptedException e) {
            LOG.error("Get MQ znode data Failed!");
            e.printStackTrace();
        }
        return bytes;
    }

    /**
     * 更新节点数据
     */
    public void setData(boolean data) {
        byte[] bytes = String.valueOf(data).getBytes();
        try {
            //"-1"表示忽略版本
            zooKeeper.setData(path, bytes, -1);
        } catch (KeeperException | InterruptedException e) {
            LOG.error("Failed to modify node data!");
            e.printStackTrace();
        }
    }
}
