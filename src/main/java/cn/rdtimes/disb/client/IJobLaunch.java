package cn.rdtimes.disb.client;

import cn.rdtimes.disb.core.IJobFuture;
import cn.rdtimes.disb.master.IJobManager;

/**
 * 客户端发布和控制任务接口
 * Created by BZ on 2019/2/13.
 */
interface IJobLaunch {

    /**
     * 启动任务,并返回任务编号
     * @param jobConf
     * @return
     * @throws Exception
     */
    IJobFuture startJob(BJobConf jobConf) throws Exception;

    /**
     * 根据任务编号停止任务
     * @param jobId
     * @throws Exception
     */
    IJobFuture stopJob(String jobId) throws Exception;

    /**
     * 设置任务报告监听
     * @param jobReportListener
     */
    void setJobReportListener(IJobManager.IJobReportListener jobReportListener);

}
