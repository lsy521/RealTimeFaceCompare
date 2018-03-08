package com.hzgc.ftpserver.captureSubscription;

import com.hzgc.util.common.FileUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * FTP演示功能（临时）
 */
public class FTPShow {
    private static List<String> ipcIdList = new ArrayList<>();

    public FTPShow(){
        List<String> list = new ArrayList<>();
        Properties properties = new Properties();
        try {
            FileInputStream fis = new FileInputStream(FileUtil.loadResourceFile("rocketmq.properties"));
            properties.load(fis);
            String show_ipcid = properties.getProperty("show_ipcid");
            list = Arrays.asList(show_ipcid.split(","));
        } catch (IOException e) {
            e.printStackTrace();
        }
        setIpcIdList(list);
    }

    public static List<String> getIpcIdList() {
        return ipcIdList;
    }

    private void setIpcIdList(List<String> ipcIdList) {
        FTPShow.ipcIdList = ipcIdList;
    }
}
