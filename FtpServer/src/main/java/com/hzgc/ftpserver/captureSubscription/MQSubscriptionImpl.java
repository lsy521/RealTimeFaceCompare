package com.hzgc.ftpserver.captureSubscription;

import com.hzgc.dubbo.address.MQSwitch;
import com.hzgc.ftpserver.util.ZookeeperClient;
import com.hzgc.util.common.FileUtil;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class MQSubscriptionImpl implements MQSwitch {
    private static Logger LOG = Logger.getLogger(MQSubscriptionImpl.class);
    private static final String path = "/mq_subscription";
    private ZookeeperClient zookeeperClient;

    /*public MQSubscriptionImpl(){
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(FileUtil.loadResourceFile("rocketmq.properties")));
            String zookeeperAddress = properties.getProperty("zookeeperAddress");
            zookeeperClient = new ZookeeperClient(10000, zookeeperAddress, path, false);
        } catch (IOException e) {
            LOG.error("zookeeperAddress no found in the \"rocketmq.properties\"");
            e.printStackTrace();
        }
    }*/
    public MQSubscriptionImpl() {
        String zookeeperAddress = "172.18.18.100:2181,172.18.18.101:2181,172.18.18.102:2181";
        zookeeperClient = new ZookeeperClient(1000, zookeeperAddress, path, false);
    }

    public static String getPath() {
        return path;
    }

    /**
     * 创建MQ存储节点
     */
    public void create() {
        zookeeperClient.create();
    }

    /**
     * 打开MQ接收数据
     *
     * @param userId    用户ID
     * @param time      时间戳
     * @param ipcIdList 设备ID列表
     */
    @Override
    public void openMQReception(String userId, long time, List<String> ipcIdList) {
        if (!userId.equals("") && !ipcIdList.isEmpty()) {
            String childPath = path + "/" + userId;
            StringBuilder data = new StringBuilder();
            data.append(userId).append(",").append(time).append(",");
            for (String ipcId : ipcIdList) {
                data.append(ipcId).append(",");
            }
            zookeeperClient.create(childPath, data.toString().getBytes());
        }
    }

    /**
     * 关闭MQ接收数据
     *
     * @param userId 用户ID
     */
    @Override
    public void closeMQReception(String userId) {
        if (!userId.equals("")) {
            zookeeperClient.delete(path + "/" + userId);
        }
    }
}
