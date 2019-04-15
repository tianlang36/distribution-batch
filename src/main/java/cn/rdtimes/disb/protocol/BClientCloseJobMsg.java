package cn.rdtimes.disb.protocol;

import cn.rdtimes.disb.core.BJobState;

import java.io.Serializable;

/**
 * 客户端关闭和汇总消息体
 * Created by BZ.
 */
public class BClientCloseJobMsg implements Serializable {
    private final static long serialVersionUID = -1;

    //任务编号
    private String jobId;
    /**
     * 成功完成任务的节点数量
     */
    private int successCount;
    /**
     * 失败任务的节点数量
     */
    private int failureCount;
    /**
     * 被停止或取消的节点数量
     */
    private int canceledCount;
    /**
     * 执行任务的所有节点数量
     */
    private int totalCount;
    //异常信息
    private String cause;

    public BClientCloseJobMsg() {
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public int getSuccessCount() {
        return successCount;
    }

    public void setSuccessCount(int successCount) {
        this.successCount = successCount;
    }

    public int getFailureCount() {
        return failureCount;
    }

    public void setFailureCount(int failureCount) {
        this.failureCount = failureCount;
    }

    public int getCanceledCount() {
        return canceledCount;
    }

    public void setCanceledCount(int canceledCount) {
        this.canceledCount = canceledCount;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public String getCause() {
        return cause;
    }

    public void setCause(String cause) {
        this.cause = cause;
    }

    public String toString() {
        return "jobId:" + jobId + "; successCount:" + successCount +
                "; failureCount:" + failureCount + "; canceledCount:" + canceledCount +
                "; totalCount:" + totalCount + "; cause:" + cause;
    }

}
