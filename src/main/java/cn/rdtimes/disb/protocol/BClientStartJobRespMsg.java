package cn.rdtimes.disb.protocol;

import cn.rdtimes.disb.core.BJobState;

import java.io.Serializable;

/**
 * 客户端启动任务响应消息体
 * Created by BZ.
 */
public class BClientStartJobRespMsg implements Serializable {
    private final static long serialVersionUID = -1;

    private String jobId;
    private long startTime;
    private BJobState jobState = BJobState.NONE;
    private int nodeCount;
    private Throwable cause;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public BJobState getJobState() {
        return jobState;
    }

    public void setJobState(BJobState jobState) {
        this.jobState = jobState;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public void setNodeCount(int nodeCount) {
        this.nodeCount = nodeCount;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public String toString() {
        return "jobId:" + jobId + "; startTime:" + startTime + "; jobState:" + jobState.name() +
                "; nodeCount:" + nodeCount;
    }

}
