package com.hzgc.ftpserver.address;

import com.hzgc.dubbo.address.MQSwitch;
import com.hzgc.ftpserver.util.ZookeeperClient;
import com.hzgc.util.common.FileUtil;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class MQSwitchImpl implements MQSwitch{
    private static Logger LOG = Logger.getLogger(MQSwitchImpl.class);
    //session失效时间
    private static int session_timeout = 1000;
    //Zookeeper地址
    private static String zookeeperAddress;
    //Zookeeper节点路径
    private static String path = "/mq_ipcid";
    //注册在path上的Watcher,节点变更会通知会向客户端发起通知
    private static boolean watcher = false;

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

    private static ZookeeperClient zookeeperClient = new ZookeeperClient(session_timeout,zookeeperAddress,path,watcher);

    /**
     * 创建MQ存储节点
     */
    public void create(){
        zookeeperClient.create();
    }

    /**
     * 打开MQ接收数据
     *
     * @param ipcIdList 设备ID列表
     */
    @Override
    public void openMQReception(List<String> ipcIdList) {
        StringBuilder ipcIds = new StringBuilder();
        if (!ipcIdList.isEmpty()){
            for (String ipcId : ipcIdList){
                ipcIds.append(ipcId).append(",");
            }
            zookeeperClient.setData(ipcIds.toString().getBytes());
        }
    }

    /**
     * 关闭MQ接收数据
     */
    @Override
    public void closeMQReception() {
        zookeeperClient.setData(null);
    }

    public static ZookeeperClient getZookeeperClient() {
        return zookeeperClient;
    }
}
