package com.hzgc.ftpserver.captureSubscription;


import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * 配置文件properties自动加载类
 */
public class PropertiesAutoLoad {
    private static Logger LOG = Logger.getLogger(PropertiesAutoLoad.class);

    /**
     * Singleton
     */
    private static final PropertiesAutoLoad AUTO_LOAD = new PropertiesAutoLoad();

    /**
     * Configuration
     */
    private static PropertiesConfiguration propConfig;

    /**
     * 自动保存
     */
    private static boolean autoSave = true;

    /**
     * 构造器私有化
     */
    private PropertiesAutoLoad() {

    }

    /**
     * properties文件路径
     *
     * @param propertiesFile
     * @return
     */
    public static PropertiesAutoLoad getInstance(String propertiesFile) {
        init(propertiesFile);
        return AUTO_LOAD;
    }

    /**
     * 初始化
     *
     * @param propertiesFile
     */
    private static void init(String propertiesFile) {
        try {
            propConfig = new PropertiesConfiguration(propertiesFile);

            //自动重新加载
            propConfig.setReloadingStrategy(new FileChangedReloadingStrategy());

            //自动保存
            propConfig.setAutoSave(autoSave);
        } catch (ConfigurationException e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * 根据Key获得对应的value
     *
     * @param key
     * @return
     */
    public Object getValueFromPropFile(String key) {
        return propConfig.getProperty(key);
    }

    /**
     * 获得对应的value数组
     *
     * @param key
     * @return
     */
    public String[] getArrayFromPropFile(String key) {
        return propConfig.getStringArray(key);
    }

    /**
     * 设置属性
     *
     * @param key
     * @param value
     */
    public void setProperty(String key, String value) {
        propConfig.setProperty(key, value);
    }

    /**
     * 设置属性
     *
     * @param map
     */
    public void setProperty(Map<String, String> map) {
        for (String key : map.keySet()) {
            propConfig.setProperty(key, map.get(key));
        }
    }
}
