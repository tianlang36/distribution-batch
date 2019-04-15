package cn.rdtimes.disb.client;

import cn.rdtimes.disb.core.*;
import cn.rdtimes.disb.protocol.*;
import cn.rdtimes.util.BStringUtil;

import java.io.IOException;

/**
 * 将和主服务器进行任务管理的交互
 * Created by BZ.
 */
class BJobLaunchImp implements IJobLaunch {
    private final BJobConnect jobConnect;

    BJobLaunchImp() {
        jobConnect = new BJobConnect(new JobMessageListenerImp());
    }

    private final Object startLock = new Object();

    public IJobFuture startJob(BJobConf jobConf) throws Exception {
        if (!jobConnect.connect(jobConf.getMasterIp(), jobConf.getMasterPort())) {
            throw new IOException("connect master is error");
        }

        try {
            //写启动任务消息
            BClientStartJobMsg startJobMsg = new BClientStartJobMsg();
            startJobMsg.setJobClassName(jobConf.getJobClassName());
            startJobMsg.setInputSplit(jobConf.getInputSplitClassName());
            BMessage<BClientStartJobMsg> message = new BMessage<BClientStartJobMsg>(BMessageCommand
                    .CLIENT_START_JOB, 0, startJobMsg);
            jobConnect.writeMessage(message);
        } catch (Exception e) {
            jobConnect.close();
            throw e;
        }

        //等待响应返回
        synchronized (startLock) {
            try {
                startLock.wait();
            } catch (Exception e) {
                //ignore
            }
        }

        return startJobFuture;
    }

    public IJobFuture stopJob(String jobId) throws Exception {
        if (jobState == BJobState.COMPLETED || jobState == BJobState.NONE ||
                BStringUtil.isEmpty(jobId)) {
            throw new IllegalStateException("job is not running");
        }

        stopJobFuture = new BDefaultJobFuture(jobId);
        try {
            //写停止任务消息
            BClientStopJobMsg stopJobMsg = new BClientStopJobMsg();
            stopJobMsg.setJobId(jobId);
            BMessage<BClientStopJobMsg> message = new BMessage<BClientStopJobMsg>(BMessageCommand
                    .CLIENT_STOP_JOB, 0, stopJobMsg);
            jobConnect.writeMessage(message);
        } catch (Exception e) {
            jobConnect.close();

            if (startJobFuture != null) {
                startJobFuture.setCompleted(null);
            }

            throw e;
        }

        return stopJobFuture;
    }

    interface JobMessageListener {
        /**
         * 启动任务响应指令通知
         *
         * @param startJobRespMsg 响应消息
         */
        void startJobResp(BClientStartJobRespMsg startJobRespMsg);

        /**
         * 关闭指令通知
         *
         * @param clientCloseJobMsg 为空值时表示连接断开
         */
        void close(BClientCloseJobMsg clientCloseJobMsg);

        /**
         * 心跳或汇报通知
         */
        void heartbeat();
    }

    private volatile BJobState jobState = BJobState.NONE;
    private volatile String jobId;
    private volatile IJobFuture startJobFuture;
    private volatile IJobFuture stopJobFuture;

    private class JobMessageListenerImp implements JobMessageListener {

        public void startJobResp(BClientStartJobRespMsg startJobRespMsg) {
            jobId = startJobRespMsg.getJobId();
            jobState = startJobRespMsg.getJobState();
            startJobFuture = new BDefaultJobFuture(startJobRespMsg.getJobId());

            //查看是否启动异常
            if (startJobRespMsg.getJobState() == BJobState.EXCEPTION) {
                BJobReport jobReport = new BJobReport();
                jobReport.setCause(startJobRespMsg.getCause() == null ? "Exception" : startJobRespMsg
                        .getCause().getMessage());
                jobReport.setTotalCount(1);
                jobReport.setFailureCount(1);
                startJobFuture.setCompleted(jobReport);
                jobConnect.close();
            }

            synchronized (startLock) {
                startLock.notify();
            }
        }

        private boolean isReceivedCloseMsg;
        public void close(BClientCloseJobMsg clientCloseJobMsg) {
            BJobLaunchImp.this.jobState = BJobState.COMPLETED;

            BJobReport jobReport = new BJobReport();
            if (clientCloseJobMsg != null) { //正常关闭
                isReceivedCloseMsg = true;
                jobReport.setJobId(clientCloseJobMsg.getJobId());
                jobReport.setTotalCount(clientCloseJobMsg.getTotalCount());
                jobReport.setFailureCount(clientCloseJobMsg.getFailureCount());
                jobReport.setCanceledCount(clientCloseJobMsg.getCanceledCount());
                jobReport.setSuccessCount(clientCloseJobMsg.getSuccessCount());
                jobReport.setCause(clientCloseJobMsg.getCause());
            } else { //非正常关闭
                if (!isReceivedCloseMsg) { //如果未接到关闭消息就是断开连接
                    jobReport.setJobId(jobId);
                    jobReport.setFailureCount(1);
                    jobReport.setTotalCount(1);
                    jobReport.setCause("connection had been closed");
                }
            }

            if (startJobFuture != null) {
                startJobFuture.setCompleted(jobReport);
            }
            if (stopJobFuture != null) {
                stopJobFuture.setCompleted(jobReport);
            }

            jobConnect.close();
        }

        public void heartbeat() {
            BInternalLogger.debug(BJobLaunchImp.class, "Client receive a heartbeat message.");
        }

    }

}
