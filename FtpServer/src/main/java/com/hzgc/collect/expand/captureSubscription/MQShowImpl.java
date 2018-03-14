package com.hzgc.collect.expand.captureSubscription;

import com.hzgc.collect.expand.util.ZookeeperClient;
import com.hzgc.dubbo.address.MQShow;
import com.hzgc.util.common.FileUtil;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class MQShowImpl implements MQShow {
    private static Logger LOG = Logger.getLogger(MQShowImpl.class);
    private String zookeeperAddress;
    private static final String path = "/mq_show";
    private ZookeeperClient zookeeperClient;

    public MQShowImpl() {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(FileUtil.loadResourceFile("rocketmq.properties")));
            zookeeperAddress = properties.getProperty("zookeeperAddress");
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
     * 创建MQ演示功能节点
     */
    public void createMQShowZnode() {
        zookeeperClient.create();
    }

    /**
     * 打开MQ演示功能
     *
     * @param userId
     * @param time
     * @param ipcIdList 设备列表
     */
    @Override
    public void openMQShow(String userId, long time, List<String> ipcIdList) {
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
     * 关闭MQ演示功能
     *
     * @param userId
     */
    @Override
    public void closeMQShow(String userId) {
        if (!userId.equals("")) {
            zookeeperClient.delete(path + "/" + userId);
        }
    }

    /**
     * 查询需要演示的设备列表
     *
     * @return 设备列表
     */
    @Override
    public List<String> getIpcId() {
        List<String> ipcIdList = new ArrayList<>();
        MQShowClient mqShowClient = new MQShowClient(1000, zookeeperAddress, path, false);
        mqShowClient.createConnection(zookeeperAddress, 10000);
        List<String> children = mqShowClient.getChildren();
        if (!children.isEmpty()) {
            for (String child : children) {
                String childPath = path + "/" + child;
                byte[] data = mqShowClient.getDate(childPath);
                if (data != null) {
                    String ipcIds = new String(data);
                    if (!ipcIds.equals("") && ipcIds.contains(",") && ipcIds.split(",").length >= 3) {
                        ipcIds = ipcIds.substring(0, ipcIds.length() - 1);
                        List<String> list = Arrays.asList(ipcIds.split(","));
                        ipcIdList = new ArrayList<>();
                        for (int i = 2; i < list.size(); i++) {
                            ipcIdList.add(list.get(i));
                        }
                    }
                }
            }
        }
        return ipcIdList;
    }
}
