package com.hzgc.ftpserver.util;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperClient {
    private static Logger LOG = Logger.getLogger(ZookeeperClient.class);
    //session失效时间
    private int session_timeout;
    //Zookeeper地址
    private String zookeeperAddress;
    //Zookeeper节点路径
    private String path;
    //注册在path上的Watcher,节点变更会通知会向客户端发起通知
    private boolean watcher;

    //Zookeeper变量
    private ZooKeeper zooKeeper = null;
    //信号量设置，用于等待zookeeper连接建立之后，通知阻塞程序继续向下执行
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public ZookeeperClient(int session_timeout, String zookeeperAddress, String path, boolean watcher) {
        this.session_timeout = session_timeout;
        this.zookeeperAddress = zookeeperAddress;
        this.path = path;
        this.watcher = watcher;
    }

    /**
     * 创建ZK连接
     *
     * @param connectAddr    ZK地址
     * @param sessionTimeout session失效时间
     */
    private void createConnection(String connectAddr, int sessionTimeout) {
        zookeeperClose();
        try {
            zooKeeper = new ZooKeeper(connectAddr, sessionTimeout, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    //获取事件的状态
                    Event.KeeperState keeperState = watchedEvent.getState();
                    Event.EventType eventType = watchedEvent.getType();
                    //如果是建立连接
                    if (Event.KeeperState.SyncConnected == keeperState) {
                        if (Event.EventType.None == eventType) {
                            //如果建立连接成功，则发送信号量，让后续阻塞程序向下执行
                            connectedSemaphore.countDown();
                        }
                    }
                }
            });
            //进行阻塞
            connectedSemaphore.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建ZK节点(数据赋值为null)
     */
    public void create() {
        this.createConnection(zookeeperAddress, session_timeout);
        try {
            zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOG.info("Creating MQ nodes in zookeeper is successful! path \":" + path + "\"");
        } catch (KeeperException | InterruptedException e) {
            LOG.error("Creating MQ nodes in zookeeper is failed!");
            e.printStackTrace();
        } finally {
            zookeeperClose();
        }
    }

    /**
     * 修改节点数据
     *
     * @param bytes 修改数据
     */
    public void setData(byte[] bytes) {
        this.createConnection(zookeeperAddress, session_timeout);
        try {
            //"-1"表示忽略版本
            zooKeeper.setData(path, bytes, -1);
        } catch (KeeperException | InterruptedException e) {
            LOG.error("Failed to modify node data!");
            e.printStackTrace();
        } finally {
            zookeeperClose();
        }
    }

    /**
     * 获取节点数据
     *
     * @return 数据
     */
    public byte[] getDate(){
        byte[] bytes = null;
        this.createConnection(zookeeperAddress, session_timeout);
        try {
            Stat stat = zooKeeper.exists(path, watcher);
            bytes = zooKeeper.getData(path,watcher,stat);
        } catch (KeeperException | InterruptedException e) {
            LOG.error("Failed to get node data!");
            e.printStackTrace();
        }finally {
            zookeeperClose();
        }
        return bytes;
    }
    
    /**
     * 获取MQ存储节点数据
     *
     * @return ipcIdList
     */
    public List<String> getData() {
        List<String> ipcIdList = new ArrayList<>();
        this.createConnection(zookeeperAddress, session_timeout);
        byte[] data = getDate();
        if (data != null){
            String ipcIds = new String(data);
            if (!ipcIds.equals("") && !ipcIds.contains(",")) {
                ipcIds = ipcIds.substring(0, ipcIds.length() - 1);
                ipcIdList.add(ipcIds);
            }else if (!ipcIds.equals("") && ipcIds.contains(",")){
                ipcIds = ipcIds.substring(0, ipcIds.length() - 1);
                ipcIdList = Arrays.asList(ipcIds.split(","));
            }
        }
        return ipcIdList;
    }

    /**
     * 关闭ZK连接
     */
    private void zookeeperClose() {
        if (this.zooKeeper != null) {
            try {
                this.zooKeeper.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
