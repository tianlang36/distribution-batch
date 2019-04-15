package cn.rdtimes.disb.protocol;


import java.io.Serializable;

/**
 * 客户端启动任务消息体
 * Created by BZ.
 */
public class BClientStartJobMsg implements Serializable {
    private final static long serialVersionUID = -1;

    //任务类全限名称
    private String jobClassName;
    //分片信息
    private String inputSplit;

    public BClientStartJobMsg() {}

    public String getInputSplit() {
        return inputSplit;
    }

    public void setInputSplit(String inputSplit) {
        this.inputSplit = inputSplit;
    }

    public String getJobClassName() {
        return jobClassName;
    }

    public void setJobClassName(String jobClassName) {
        this.jobClassName = jobClassName;
    }

    public String toString() {
        return "jobClassName:" + jobClassName + "; inputSplit:" + inputSplit;
    }

}
