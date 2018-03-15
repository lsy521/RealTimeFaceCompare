package com.hzgc.collect.expand.subscribe;

import com.hzgc.collect.expand.util.ClusterOverFtpProperHelper;

import java.io.Serializable;

/**
 * Zookeeper客户端连接参数
 */
public class ZookeeperParam implements Serializable{
    //session失效时间
    public static final int SESSION_TIMEOUT = 10000;
    //Zookeeper地址
    public static String zookeeperAddress = "172.18.18.163:2181";
    //订阅节点路径
    public static final String PATH_SUBSCRIBE = "/mq_subscribe";
    //演示节点路径
    public static final String PATH_MQSHOW = "/mq_show";
    //总开关路径
    public static final String PATH_SWITCH = "/mq_switch";
    //注册在path上的Watcher,节点变更会通知会向客户端发起通知
    public static final boolean WATCHER = false;
}
