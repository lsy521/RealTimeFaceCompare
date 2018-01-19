package com.hzgc.ftpserver.captureSubscription;

import com.hzgc.ftpserver.util.ZookeeperClient;
import com.hzgc.util.common.FileUtil;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Zookeeper客户端初始化
 */
public class MQSwitchInit {
    private static Logger LOG = Logger.getLogger(MQSwitchInit.class);
    private static int session_timeout = 1000;
    private static String zookeeperAddress;
    private static boolean watcher = false;
    protected static String path = "/mq_ipcid";
    protected static ZookeeperClient zookeeperClient = new ZookeeperClient(session_timeout, zookeeperAddress, path, watcher);

    static {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(FileUtil.loadResourceFile("rocketmq.properties")));
            zookeeperAddress = properties.getProperty("zookeeperAddress");
        } catch (IOException e) {
            LOG.error("zookeeperAddress no found in the \"rocketmq.properties\"");
            e.printStackTrace();
        }
    }

    public ZookeeperClient getZookeeperClient() {
        return zookeeperClient;
    }

    /**
     * 创建MQ存储节点
     */
    public static void create() {
        zookeeperClient.create();
    }
}
