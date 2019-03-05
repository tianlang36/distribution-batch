package cn.rdtimes.disb.protocol;

import java.io.Serializable;

/**
 * 停止任务消息体
 * Created by BZ on 2019/2/13.
 */
public class BStopJobMsg implements Serializable {
    private final static long serialVersionUID = -1;

    //任务编号
    private String jobId;

    public BStopJobMsg() {}

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
