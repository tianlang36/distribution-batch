package cn.rdtimes.disb.client;

/**
 * 客户端任务配置
 * Created by BZ.
 */
public class BJobConf {
    private String jobClassName;
    private String inputSplitClassName;

    private String masterIp;
    private int masterPort;

    public BJobConf() {}

    public String getInputSplitClassName() {
        return inputSplitClassName;
    }

    public void setInputSplitClassName(String inputSplitClassName) {
        this.inputSplitClassName = inputSplitClassName;
    }

    public String getJobClassName() {
        return jobClassName;
    }

    public void setJobClassName(String jobClassName) {
        this.jobClassName = jobClassName;
    }

    public String getMasterIp() {
        return masterIp;
    }

    public void setMasterIp(String masterIp) {
        this.masterIp = masterIp;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }

}
