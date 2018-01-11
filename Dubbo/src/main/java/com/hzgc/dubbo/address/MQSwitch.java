package com.hzgc.dubbo.address;

import java.util.List;

public interface MQSwitch {
    /**
     * 打开MQ接收数据
     *
     * @param ipcIdList 设备ID列表
     */
    void openMQReception(List<String> ipcIdList);

    /**
     * 关闭MQ接收数据
     */
    void closeMQReception();
}
