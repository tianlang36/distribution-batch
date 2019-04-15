package cn.rdtimes.disb.client;

import cn.rdtimes.disb.core.IJobFuture;

/**
 * 客户端发布和控制任务接口
 * Created by BZ.
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


}
