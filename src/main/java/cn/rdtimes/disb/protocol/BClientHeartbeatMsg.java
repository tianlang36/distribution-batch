package cn.rdtimes.disb.protocol;

import cn.rdtimes.disb.core.BJobState;

import java.io.Serializable;

/**
 * 客户端心跳或汇报消息体
 * Created by BZ.
 */
public class BClientHeartbeatMsg implements Serializable {
    private final static long serialVersionUID = -1;

    //任务编号
    private String jobId;

    public BClientHeartbeatMsg() {}

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String toString() {
        return "jobId:" + jobId ;
    }

}
