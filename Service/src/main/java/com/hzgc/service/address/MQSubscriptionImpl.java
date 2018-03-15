package com.hzgc.service.address;

import com.hzgc.collect.expand.subscribe.ZookeeperParam;
import com.hzgc.collect.expand.util.ZookeeperClient;
import com.hzgc.dubbo.address.MQSubscription;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;

public class MQSubscriptionImpl implements MQSubscription, Serializable {
    private static Logger LOG = Logger.getLogger(MQSubscriptionImpl.class);
    private ZookeeperClient zookeeperClient;

    public MQSubscriptionImpl() {
        zookeeperClient = new ZookeeperClient(ZookeeperParam.SESSION_TIMEOUT, ZookeeperParam.zookeeperAddress,
                ZookeeperParam.PATH_SUBSCRIBE, ZookeeperParam.WATCHER);
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
            String childPath = ZookeeperParam.PATH_SUBSCRIBE + "/" + userId;
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
            zookeeperClient.delete(ZookeeperParam.PATH_SUBSCRIBE + "/" + userId);
        }
    }
}
