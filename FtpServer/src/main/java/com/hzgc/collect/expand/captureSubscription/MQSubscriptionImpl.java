package com.hzgc.collect.expand.captureSubscription;

import com.hzgc.collect.expand.util.ZookeeperClient;
import com.hzgc.dubbo.address.MQSubscription;
import com.hzgc.util.common.FileUtil;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class MQSubscriptionImpl implements MQSubscription {
    private static Logger LOG = Logger.getLogger(MQSubscriptionImpl.class);
    private static final String path = "/mq_subscription";
    private ZookeeperClient zookeeperClient;

    public MQSubscriptionImpl(){
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(FileUtil.loadResourceFile("rocketmq.properties")));
            String zookeeperAddress = properties.getProperty("zookeeperAddress");
            zookeeperClient = new ZookeeperClient(10000, zookeeperAddress, path, false);
        } catch (IOException e) {
            LOG.error("zookeeperAddress no found in the \"rocketmq.properties\"");
            e.printStackTrace();
        }
    }

    public static String getPath() {
        return path;
    }

    /**
     * 创建MQ存储节点
     */
    public void createMQSubscriptionZnode() {
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
