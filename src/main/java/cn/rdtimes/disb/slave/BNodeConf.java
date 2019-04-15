package cn.rdtimes.disb.slave;

import cn.rdtimes.disb.core.BInternalLogger;
import cn.rdtimes.util.BPropertiesUtil;
import cn.rdtimes.util.BStringUtil;

import java.util.Properties;

/**
 * 节点配置信息
 * 单件模式
 * Created by BZ.
 */
final class BNodeConf {
    private final static String FILE_NAME = "slave.properties";
    private final static BNodeConf instance = new BNodeConf();

    private String nodeName;
    private String masterIp;
    private int masterPort;
    //单位:毫秒
    private int heartbeatInterval;
    //单位:毫秒
    private int tryMaxWaitConnection;
    private int jobMaxRunning;

    BNodeConf() {}

    static BNodeConf getInstance() {
        return instance;
    }

    void readConf(String fileName) {
        if (BStringUtil.isEmpty(fileName)) {
            fileName = FILE_NAME;
        }

        Properties properties = BPropertiesUtil.getProperties(fileName);
        if (properties == null) {
            throw new IllegalArgumentException("load properties file error");
        }

        nodeName = properties.getProperty("nodeName");
        if (BStringUtil.isEmpty(nodeName)) {
            throw new IllegalArgumentException("nodeName is null");
        }
        masterIp = properties.getProperty("masterIp", "127.0.0.1");
        masterPort = getInt(properties.getProperty("masterPort", "21999"));
        jobMaxRunning = getInt(properties.getProperty("jobMaxRunning", "1"));
        heartbeatInterval = getInt(properties.getProperty("heartbeatInterval", "5")) * 1000;
        tryMaxWaitConnection = getInt(properties.getProperty("tryMaxWaitConnection", "1200")) * 1000;
        BInternalLogger.logLevel = BInternalLogger.getLogLevel(properties.getProperty("logLevel", "DEBUG"));
    }

    private int getInt(String src) {
        return Integer.parseInt(src);
    }

    String getNodeName() {
        return nodeName;
    }

    String getMasterIp() {
        return masterIp;
    }

    int getMasterPort() {
        return masterPort;
    }

    int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    int getTryMaxWaitConnection() {
        return tryMaxWaitConnection;
    }

    int getJobMaxRunning() {
        return jobMaxRunning;
    }

}
