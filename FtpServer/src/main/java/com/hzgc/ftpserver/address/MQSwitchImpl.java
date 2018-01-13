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
    private static String zookeeperAddress = "172.18.18.103:2181,172.18.18.104:2181,172.18.18.105:2181";
    //Zookeeper节点路径
    private static String path = "/mq_ipcid";
    //注册在path上的Watcher,节点变更会通知会向客户端发起通知
    private static boolean watcher = false;

   /* static {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(FileUtil.loadResourceFile("rocketmq.properties")));
            zookeeperAddress = properties.getProperty("zookeeperAddress");
        } catch (IOException e) {
            LOG.error("zookeeperAddress no found in the \"rocketmq.properties\"");
            e.printStackTrace();
        }
    }*/

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
     * @param userId    用户ID
     * @param ipcIdList 设备ID列表
     */
    @Override
    public void openMQReception(String userId, List<String> ipcIdList) {
        if (!userId.equals("") && !ipcIdList.isEmpty()){
            String childPath = path + "/" + userId;
            StringBuilder ipcIds = new StringBuilder();
            for (String ipcId : ipcIdList){
                ipcIds.append(ipcId).append(",");
            }
            zookeeperClient.create(childPath,ipcIds.toString().getBytes());
        }
    }

    /**
     * 关闭MQ接收数据
     *
     * @param userId 用户ID
     */
    @Override
    public void closeMQReception(String userId) {
        if (!userId.equals("")){
            String childPath = path + "/" + userId;
            zookeeperClient.delete(childPath);
        }
    }

    public static ZookeeperClient getZookeeperClient() {
        return zookeeperClient;
    }
}
