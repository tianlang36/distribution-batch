package cn.rdtimes.disb.client;

import cn.rdtimes.disb.core.*;
import cn.rdtimes.disb.master.IJobManager;
import cn.rdtimes.util.BStringUtil;

/**
 * 客户端任务提交类,只能执行一次任务,多个任务需要创建多个本对象
 * 使用代理模式,以后主服务器可以独立部署,通过消息来管理.
 * 使用说明:
 * BJob job = new BJob(new BJobConf());
 * job.setJobClassName(IJob.class);
 * job.setInputSplit(IInputSplit.class);
 * job.setOutput(true);
 * job.launchJob();
 * job.waitCompleted();
 * if (!job.isSuccess()) {
 * //错误处理
 * } else {
 * //成功处理
 * }
 * <p>
 * Created by BZ on 2019/2/12.
 */
public class BJob {
    //创建job代理对象
    private final BJobLaunchProxy jobLaunchProxy;

    //job编号
    private String id;
    //任务是否完成(不关心任务状态)
    private volatile boolean completed;
    //是否被stop的标志
    private volatile boolean stopFlag;
    //是否输出日志
    private volatile boolean isOutput;
    //异常信息
    private String cause;
    //成功完成任务节点的数量
    private int successCount;
    //失败任务节点的数量
    private int failureCount;
    //停止或取消任务节点的数量
    private int canceledCount;
    //客户端任务配置
    private BJobConf jobConf;

    public BJob() {
        this(new BJobConf());
    }

    public BJob(BJobConf jobConf) {
        if (jobConf == null) {
            throw new IllegalArgumentException("jobConf is null");
        }

        this.jobConf = jobConf;
        jobLaunchProxy = new BJobLaunchProxy();
        jobLaunchProxy.setJobReportListener(new JobReportListener());
    }

    public void setJobClass(Class<? extends IJob> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("clazz is null");
        }

        jobConf.setJobClassName(clazz.getName());
    }

    public void setInputSplit(Class<? extends IInputSplit> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("clazz is null");
        }

        jobConf.setInputSplitClassName(clazz.getName());
    }

    private boolean haveJob() {
        if (completed || !BStringUtil.isEmpty(id)) {
            return true;
        }
        return false;
    }

    public String getId() {
        return id;
    }

    public String getCause() {
        return cause;
    }

    public int getSuccessCount() {
        return successCount;
    }

    public int getFailureCount() {
        return failureCount;
    }

    public int getCanceledCount() {
        return canceledCount;
    }

    private IJobFuture startJobFuture;

    public void launchJob() throws Exception {
        if (haveJob()) {
            throw new IllegalStateException("job have been completed or is running");
        }

        startJobFuture = jobLaunchProxy.startJob(jobConf);
        id = startJobFuture.getId();

        BInternalLogger.debug(BJob.class, "It has a job (" + id + ") that is running...");
    }

    public void setOutput(boolean isOutput) {
        this.isOutput = isOutput;
    }

    public boolean isSuccess() {
        return (failureCount == 0 && !stopFlag);
    }

    public boolean isStopped() {
        return stopFlag;
    }

    public void stopJob() throws Exception {
        if (BStringUtil.isEmpty(id)) {
            throw new IllegalStateException("job id is null");
        }
        if (completed) {
            throw new IllegalStateException("job have been completed");
        }

        stopFlag = true;
        completed = true;

        IJobFuture jobFuture = jobLaunchProxy.stopJob(id);
        if (startJobFuture != null) {  //有启动等待的就结束
            startJobFuture.setCompleted(null);
        }

        BJobReport jobReport = jobFuture.waitCompleted();
        assignResult(jobReport);
    }

    private void assignResult(BJobReport jobReport) {
        if (jobReport == null) return;
        
        failureCount = jobReport.getFailureCount();
        successCount = jobReport.getSuccessCount();
        cause = jobReport.getCause();
        canceledCount = jobReport.getCanceledCount();
    }

    public void waitCompleted() {
        if (completed) {
            throw new IllegalStateException("job have been completed");
        }

        if (startJobFuture != null) {
            BJobReport jobReport = startJobFuture.waitCompleted();
            completed = true;
            assignResult(jobReport);
        }

        startJobFuture = null;
    }

    public String toString() {
        return "id:" + id + "; isSuccess:" + isSuccess() + "; isStopped:" + isStopped() +
                "; successCount:" + successCount + "; failureCount:" + failureCount +
                "; canceledCount:" + canceledCount + "; cause:" + cause;
    }

    class JobReportListener implements IJobManager.IJobReportListener {

        public void jobReport(String jobId, BJobReport jobReport) {
            if (!isOutput) return;

            BInternalLogger.info(JobReportListener.class, jobReport.toString());
        }

    }

}
