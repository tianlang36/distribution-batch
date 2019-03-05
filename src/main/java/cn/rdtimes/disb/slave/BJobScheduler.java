package cn.rdtimes.disb.slave;

import cn.rdtimes.disb.core.BInternalLogger;
import cn.rdtimes.disb.core.BJobState;
import cn.rdtimes.disb.core.BSplit;
import cn.rdtimes.disb.protocol.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 任务调度器
 * 功能:
 * 1.启动和停止任务
 * 2.定时汇报任务状态
 * 3.管理连接管理
 * 4.写即时完成消息
 * Created by BZ on 2019/2/14.
 */
final class BJobScheduler {
    private final BConnectManager connectManager;

    //等待运行job队列
    private final ConcurrentLinkedQueue<BJobRunInfo> pendingQueue;
    //正在运行或完成任务队列
    private final List<BJobRunInfo> runningAndCompletionList;
    //运行任务容器map,key为任务编号
    private final ConcurrentHashMap<String, BJobContainer> containerMap;

    //线程停止标志
    private volatile boolean stopFlag;
    //线程锁
    private final Object lock = new Object();

    BJobScheduler() {
        //最大并行运行job数量
        int count = BNodeConf.getInstance().getJobMaxRunning();

        pendingQueue = new ConcurrentLinkedQueue<BJobRunInfo>();
        runningAndCompletionList = Collections.synchronizedList(new ArrayList<BJobRunInfo>(count));
        containerMap = new ConcurrentHashMap<String, BJobContainer>(count);

        connectManager = new BConnectManager(new BCommandListenerImp());
    }

    void start() {
        JobSchedulerThread jobThread = new JobSchedulerThread();
        jobThread.start();

        connectManager.start();

        HeartbeatThread heartbeatThread = new HeartbeatThread();
        heartbeatThread.start();
    }

    void shutdown() {
        stopFlag = true;
        synchronized (lock) {
            lock.notify();
        }
        synchronized (pendingQueue) {
            pendingQueue.notify();
        }

        connectManager.close();
    }

    void registryContainer(String jobId, BJobContainer container) {
        containerMap.put(jobId, container);
    }

    void removeContainer(String jobId) {
        containerMap.remove(jobId);

        //通知等待的
        synchronized (pendingQueue) {
            pendingQueue.notify();
        }
    }

    private void addQueue(BJobRunInfo jobInfo) {
        pendingQueue.offer(jobInfo);
        synchronized (pendingQueue) {
            pendingQueue.notify();
        }
    }

    void notifyHaveCompleteMsg() {
        if (stopFlag) return;

        synchronized (lock) {
            lock.notify();
        }
    }

    private BJobInfoMsg getJobInfoMsg(BJobRunInfo jobRunInfo) {
        BJobInfoMsg jobInfoMsg = new BJobInfoMsg();
        jobInfoMsg.setJobId(jobRunInfo.getJobId());
        jobInfoMsg.setState(jobRunInfo.getJobState());
        jobInfoMsg.setCause(jobRunInfo.getCause());
        jobInfoMsg.setStartTime(jobRunInfo.getStartTime());
        jobInfoMsg.setEndTime(jobRunInfo.getEndTime());

        return jobInfoMsg;
    }

    //实现指令监听处理
    private class BCommandListenerImp implements ICommandListener {

        public void startJob(BStartJobMsg startJobMsg) {
            BJobRunInfo jobRunInfo = new BJobRunInfo(startJobMsg.getJobId(),
                    startJobMsg.getJobClassName(), startJobMsg.getSplit());
            addQueue(jobRunInfo);
        }

        public void stopJob(BStopJobMsg stopJobMsg) {
            String jobId = stopJobMsg.getJobId();
            if (containerMap.containsKey(jobId)) {
                containerMap.get(jobId).stop();
            }
        }

    }

    //任务调度线程
    private class JobSchedulerThread extends Thread {
        JobSchedulerThread() {
            super("Node-JobSchedulerThread");
        }

        public void run() {
            BInternalLogger.info(BJobScheduler.class, getName() + " have been started.");

            int maxJob = BNodeConf.getInstance().getJobMaxRunning();
            while (!stopFlag) {
                //当队列为0或运行的容器已到最大
                if (pendingQueue.size() == 0 || containerMap.size() >= maxJob) {
                    BInternalLogger.debug(BJobScheduler.class, getName() + " is waiting job...");

                    synchronized (pendingQueue) {
                        try {
                            pendingQueue.wait();
                        } catch (InterruptedException e) {
                            //ignore
                        }
                    }
                }

                if (stopFlag) break;

                BJobRunInfo jobRunInfo = pendingQueue.poll();
                if (jobRunInfo == null) continue;

                boolean isSuccess = true;
                BJobContainer container;
                try {
                    container = new BJobContainer(BJobScheduler.this, jobRunInfo);
                    container.start();
                } catch (Exception e) { //通常在容器启动前出现了异常,这时容器还没有真正运行任务
                    BInternalLogger.error(BJobScheduler.class, "jobContainer start error:", e);

                    jobRunInfo.setJobState(BJobState.EXCEPTION);
                    jobRunInfo.setCause(e);
                    isSuccess = false;
                } finally {
                    runningAndCompletionList.add(jobRunInfo);
                    //异常时写即时通知完成消息,非异常时任务容器会通知写消息
                    if (!isSuccess) {
                        notifyHaveCompleteMsg();
                    }
                }
            }

            BInternalLogger.info(BJobScheduler.class, getName() + " is over.");
        }

    }

    //心跳线程或汇报线程
    private class HeartbeatThread extends Thread {
        HeartbeatThread() {
            super("Node-HeartbeatThread");
        }

        public void run() {
            BInternalLogger.info(BJobScheduler.class, getName() + " have been started.");
            int interval = BNodeConf.getInstance().getHeartbeatInterval();

            while (!stopFlag) {
                synchronized (lock) {
                    try {
                        lock.wait(interval);
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }

                if (stopFlag) break;

                //发送汇报消息,并删除已完成的job
                sendReportToMaster();
            }

            BInternalLogger.info(BJobScheduler.class, getName() + " is over.");
        }

        void sendReportToMaster() {
            BReportJobMsg reportJobMsg = new BReportJobMsg();
            BMessage<BReportJobMsg> message = new BMessage<BReportJobMsg>(BMessageCommand.REPORT_HEARTBEAT,
                    0, reportJobMsg);
            int count = 0;

            //先遍历队列中的
            Iterator<BJobRunInfo> iterator = pendingQueue.iterator();
            while (iterator.hasNext()) {
                reportJobMsg.add(getJobInfoMsg(iterator.next()));
                ++count;
            }

            //再遍历列表中的
            iterator = runningAndCompletionList.iterator();
            while (iterator.hasNext()) {
                BJobRunInfo jobRunInfo = iterator.next();
                reportJobMsg.add(getJobInfoMsg(jobRunInfo));
                ++count;

                //删除已完成的
                if (jobRunInfo.getJobState() == BJobState.COMPLETED ||
                        jobRunInfo.getJobState() == BJobState.EXCEPTION ||
                        jobRunInfo.getJobState() == BJobState.STOPPED) {
                    iterator.remove();
                }
            }

            reportJobMsg.setJobCount(count);
            reportJobMsg.setNodeName(BNodeConf.getInstance().getNodeName());

            connectManager.writeMessage(message);
        }

    }

    //运行job相关信息
    class BJobRunInfo {
        private BJobState jobState = BJobState.QUEUEING;
        private Throwable cause;
        private final String jobId;
        private final String jobClassName;
        private final BSplit split;
        private long startTime;
        private long endTime;

        BJobRunInfo(String jobId, String jobClassName, BSplit split) {
            this.jobId = jobId;
            this.jobClassName = jobClassName;
            this.split = split;
        }

        BJobState getJobState() {
            return jobState;
        }

        void setJobState(BJobState jobState) {
            this.jobState = jobState;
        }

        String getJobId() {
            return jobId;
        }

        String getJobClassName() {
            return jobClassName;
        }

        BSplit getSplit() {
            return split;
        }

        Throwable getCause() {
            return cause;
        }

        void setCause(Throwable cause) {
            this.cause = cause;
        }

        long getStartTime() {
            return startTime;
        }

        void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        long getEndTime() {
            return endTime;
        }

        void setEndTime(long endTime) {
            this.endTime = endTime;
        }

    }

}
