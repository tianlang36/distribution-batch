package cn.rdtimes.disb.master;

import cn.rdtimes.disb.core.BJobReport;
import cn.rdtimes.disb.core.IJobFuture;

/**
 * 任务管理接口,主要是服务client端请求
 * <p>
 * Created by BZ on 2019/2/19.
 */
public interface IJobManager {

    /**
     * 启动任务到每个节点服务器
     *
     * @return
     */
    IJobFuture startJob(String jobClassName, String inputSplitClassName) throws Exception;

    /**
     * 停止任务到每个节点服务器
     *
     * @param jobId
     * @return
     */
    IJobFuture stopJob(String jobId) throws Exception;

    /**
     * 设置任务汇报监听(向客户端通知)
     * @param jobReportListener
     */
    void setJobReportListener(IJobReportListener jobReportListener);

    //任务汇报监听
    interface IJobReportListener {

        /**
         * 任务状态报告通知
         *
         * @param jobId
         * @param jobReport
         */
        void jobReport(String jobId, BJobReport jobReport);

    }

}
