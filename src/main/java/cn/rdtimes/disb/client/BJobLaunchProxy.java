package cn.rdtimes.disb.client;

import cn.rdtimes.disb.core.IJobFuture;

/**
 * 代理模式
 * Created by BZ.
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

}
