package cn.rdtimes.disb.core;

import java.io.Serializable;

/**
 * 任务状态报告
 * 应该是所有节点汇总的状态
 * Created by BZ on 2019/2/19.
 */
public class BJobReport implements Serializable {
    private final static long serialVersionUID = -1;
    //心跳值
    private final static int  HEATBEAT = 0xFFFFFF;

    /**
     * 任务编号
     */
    private String jobId;
    /**
     * 任务执行进度,按节点进行计算,最大为100
     */
    private int progress;
    /**
     * 如果发生异常,异常报告,多个异常使用"|"分隔
     */
    private String cause;
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
     * 正在运行的节点数量
     */
    private int runningCount;
    /**
     * 执行任务的所有节点数量
     */
    private int totalCount;

    public BJobReport() {}

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        if (progress < 0) progress = 0;
        this.progress = progress;
    }

    public String getCause() {
        return cause;
    }

    public void setCause(String cause) {
        this.cause = cause;
    }

    public int getSuccessCount() {
        return successCount;
    }

    public void setSuccessCount(int successCount) {
        if (successCount < 0) return;

        this.successCount = successCount;
    }

    public int getFailureCount() {
        return failureCount;
    }

    public void setFailureCount(int failureCount) {
        if (failureCount < 0) return;

        this.failureCount = failureCount;
    }

    public int getCanceledCount() {
        return canceledCount;
    }

    public void setCanceledCount(int canceledCount) {
        if (canceledCount < 0) return;

        this.canceledCount = canceledCount;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        if (totalCount < 0) return;

        this.totalCount = totalCount;
    }

    public int getRunningCount() {
        return runningCount;
    }

    public void setRunningCount(int runningCount) {
        if (runningCount < 0) return;

        this.runningCount = runningCount;
    }

    public boolean equalTotalCount() {
        return (totalCount == (successCount + failureCount + canceledCount));
    }

    public boolean isHeatbeat() { //使用特定值用来判断是否为心跳信息,用于客户端
        if (progress == HEATBEAT) return true;
        return false;
    }

    public String toString() {
        return "jobId:" + jobId + "; progress:" + progress + "; successCount:" + successCount +
                "; failureCount:" + failureCount + "; canceledCount:" + canceledCount +
                "; runningCount:" + runningCount + "; totalCount:" + totalCount + "; cause:" + cause;
    }

}
