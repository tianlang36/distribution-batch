package cn.rdtimes.disb.master;

import cn.rdtimes.disb.core.BInternalLogger;
import cn.rdtimes.util.BPropertiesUtil;
import cn.rdtimes.util.BStringUtil;

import java.util.Properties;

/**
 * 主服务配置信息
 * 单件模式
 * Created by BZ.
 */
final class BMasterConf {
    private final static String FILE_NAME = "master.properties";
    private final static BMasterConf instance = new BMasterConf();

    //监听端口
    private int port;
    //单位:毫秒
    private int missingNodeMaxTime;
    private int nioThreadCount;

    private int clientPort;
    private int clientNioThreadCount;
    //单位:毫秒
    private int clientHeartbeatInterval;
    private int jobCompleteFailCount;

    BMasterConf() {}

    static BMasterConf getInstance() {
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

        port = getInt(properties.getProperty("port", "21999"));
        missingNodeMaxTime = getInt(properties.getProperty("missingNodeMaxTime", "300")) * 1000;
        nioThreadCount = getInt(properties.getProperty("nioThreadCount", "2"));
        clientPort = getInt(properties.getProperty("clientPort", "21998"));
        jobCompleteFailCount = getInt(properties.getProperty("jobCompleteFailCount", "21"));
        clientNioThreadCount = getInt(properties.getProperty("clientNioThreadCount", "1"));
        clientHeartbeatInterval = getInt(properties.getProperty("clientHeartbeatInterval", "5")) * 1000;
        BInternalLogger.logLevel = BInternalLogger.getLogLevel(properties.getProperty("logLevel", "DEBUG"));
    }

    private int getInt(String src) {
        return Integer.parseInt(src);
    }

    public int getJobCompleteFailCount() {
        return jobCompleteFailCount;
    }

    int getPort() {
        return port;
    }

    int getMissingNodeMaxTime() {
        return missingNodeMaxTime;
    }

    int getNioThreadCount() {
        return nioThreadCount;
    }

    public int getClientPort() {
        return clientPort;
    }

    public int getClientNioThreadCount() {
        return clientNioThreadCount;
    }

    public int getClientHeartbeatInterval() {
        return clientHeartbeatInterval;
    }
}
