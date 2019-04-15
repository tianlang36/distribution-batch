package cn.rdtimes.disb.protocol;

import cn.rdtimes.disb.core.BJobState;

import java.io.Serializable;

/**
 * 任务基本信息消息体
 * Created by BZ.
 */
public class BJobInfoMsg implements Serializable {
    private final static long serialVersionUID = -1;

    //任务编号
    private String jobId;
    //任务状态
    private BJobState state = BJobState.NONE;
    //开始执行时间
    private long startTime;
    //结束时间
    private long endTime;
    //异常信息
    private Throwable cause;

    public BJobInfoMsg() {
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public BJobState getState() {
        return state;
    }

    public void setState(BJobState state) {
        this.state = state;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public String toString() {
        return "jobId:" + jobId + "; state:" + state.name() +
                "; startTime:" + startTime + "; endTime:" + endTime +
                "; cause:" + (cause == null ? "none" : cause.getMessage());
    }

}
