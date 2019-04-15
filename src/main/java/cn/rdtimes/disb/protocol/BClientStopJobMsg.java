package cn.rdtimes.disb.protocol;

import java.io.Serializable;

/**
 * 客户端停止任务消息体
 * Created by BZ.
 */
public class BClientStopJobMsg implements Serializable {
    private final static long serialVersionUID = -1;

    //任务编号
    private String jobId;

    public BClientStopJobMsg() {}

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String toString() {
        return "jobId:" + jobId;
    }

}
