package cn.rdtimes.disb.client;

import cn.rdtimes.disb.core.IJobFuture;
import cn.rdtimes.disb.master.BClientJobServiceFactory;
import cn.rdtimes.disb.master.IJobManager;

/**
 * 将和主服务器进行任务管理的交互
 * Created by BZ on 2019/2/13.
 */
class BJobLaunchImp implements IJobLaunch {
    private final IJobManager jobManager;

    public BJobLaunchImp() {
        jobManager = BClientJobServiceFactory.getJobManager();
    }

    public IJobFuture startJob(BJobConf jobConf) throws Exception {
        return jobManager.startJob(jobConf.getJobClassName(), jobConf.getInputSplitClassName());
    }

    public IJobFuture stopJob(String jobId) throws Exception {
        return jobManager.stopJob(jobId);
    }

    public void setJobReportListener(IJobManager.IJobReportListener jobReportListener) {
        jobManager.setJobReportListener(jobReportListener);
    }

}
