package cn.rdtimes.disb.client;

import cn.rdtimes.disb.core.*;
import cn.rdtimes.util.BStringUtil;

/**
 * 客户端任务提交类,只能执行一次任务,不能重复提交任务。
 * 多个任务需要创建多个此对象。
 * 使用代理模式.
 * 使用说明:
 * BJob job = new BJob(new BJobConf());
 * job.setMasterIp("localhost");
 * job.setMasterPort(21998);
 * job.setJobClass(IJob.class);
 * job.setInputSplitClass(IInputSplit.class);
 * job.launchJob();
 * job.waitCompleted();
 * if (!job.isSuccess()) {
 * //错误处理
 * } else {
 * //成功处理
 * }
 * <p>
 * Created by BZ.
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
    }

    public BJob setJobClass(Class<? extends IJob> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("clazz is null");
        }

        jobConf.setJobClassName(clazz.getName());
        return this;
    }

    public BJob setInputSplitClass(Class<? extends IInputSplit> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("clazz is null");
        }

        jobConf.setInputSplitClassName(clazz.getName());
        return this;
    }

    public BJob setMasterIp(String ip) {
        if (BStringUtil.isEmpty(ip)) {
            throw new IllegalArgumentException("ip is null");
        }

        jobConf.setMasterIp(ip);
        return this;
    }

    public BJob setMasterPort(int port) {
        if (port <= 0) {
            throw new IllegalArgumentException("port is than 0");
        }

        jobConf.setMasterPort(port);
        return this;
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

    private boolean haveJob() {
        if (completed || !BStringUtil.isEmpty(id)) {
            return true;
        }
        return false;
    }

    public void launchJob() throws Exception {
        if (haveJob()) {
            throw new IllegalStateException("job have been completed or is running");
        }

        startJobFuture = jobLaunchProxy.startJob(jobConf);
        id = startJobFuture.getId();

        BInternalLogger.debug(BJob.class, "It has a job (" + id + ") that is running...");
    }

    public boolean isSuccess() {
        return (successCount > 0 && failureCount == 0 && !stopFlag);
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
        IJobFuture jobFuture = jobLaunchProxy.stopJob(id);
        BJobReport jobReport = jobFuture.waitCompleted();
        assignResult(jobReport);
    }

    private void assignResult(BJobReport jobReport) {
        completed = true;
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
            assignResult(jobReport);
        }

        startJobFuture = null;
    }

    public String toString() {
        return "id:" + id + "; isSuccess:" + isSuccess() + "; isStopped:" + isStopped() +
                "; successCount:" + successCount + "; failureCount:" + failureCount +
                "; canceledCount:" + canceledCount + "; cause:" + cause;
    }

}
