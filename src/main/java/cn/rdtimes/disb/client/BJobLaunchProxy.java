package cn.rdtimes.disb.client;

import cn.rdtimes.disb.core.IJobFuture;
import cn.rdtimes.disb.master.IJobManager;

/**
 * 代理模式
 * Created by BZ on 2019/2/13.
 */
class BJobLaunchProxy implements IJobLaunch {
    private final BJobLaunchImp jobLaunchImp;

    public BJobLaunchProxy() {
        jobLaunchImp = new BJobLaunchImp();
    }

    public IJobFuture startJob(BJobConf jobConf) throws Exception {
        return jobLaunchImp.startJob(jobConf);
    }

    public IJobFuture stopJob(String jobId) throws Exception {
        return jobLaunchImp.stopJob(jobId);
    }

    public void setJobReportListener(IJobManager.IJobReportListener jobReportListener) {
        jobLaunchImp.setJobReportListener(jobReportListener);
    }

}
