package cn.rdtimes.disb.slave;


import cn.rdtimes.disb.core.BInternalLogger;
import cn.rdtimes.disb.core.BJobState;
import cn.rdtimes.disb.core.IJob;

/**
 * 任务容器,用来运行任务的.
 * 功能:
 * 1.运行job
 * 2.更新job状态(启动,停止,异常,完成)
 * 3.向调度器注册和注销自己
 * Created by BZ.
 */
final class BJobContainer {
    private final BJobScheduler.BJobRunInfo jobRunInfo;
    private final BJobScheduler jobScheduler;

    //是否被停止的标志(调用stop方法)
    private volatile boolean isStop;

    BJobContainer(BJobScheduler jobScheduler, BJobScheduler.BJobRunInfo jobRunInfo) {
        this.jobScheduler = jobScheduler;
        this.jobRunInfo = jobRunInfo;
    }

    private JobRunner jobRunner;

    void start() {
        IJob job = createJob();
        jobRunner = new JobRunner(job);
        jobScheduler.registryContainer(jobRunInfo.getJobId(), this);

        Thread jobThread = new Thread(jobRunner);
        jobThread.start();
    }

    private IJob createJob() {
        try {
            Class<?> clazz = Class.forName(jobRunInfo.getJobClassName());
            IJob job = (IJob) (clazz.newInstance());
            job.setId(jobRunInfo.getJobId());
            job.setSplit(jobRunInfo.getSplit());
            return job;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void stop() {
        isStop = true;
        if (jobRunner != null) {
            jobRunner.stop();
        }
    }

    private class JobRunner implements Runnable {
        final IJob job;

        JobRunner(IJob job) {
            this.job = job;
        }

        public void run() {
            try {
                BInternalLogger.debug(BJobContainer.class, "It have a job " +
                        "(" + job.getId() + ") that is running...");

                jobRunInfo.setJobState(BJobState.RUNNING);
                jobRunInfo.setStartTime(System.currentTimeMillis());

                //启动任务,并等待结束
                job.start();
                job.waitCompleted();

                if (isStop) { //是否被取消或停止了
                    jobRunInfo.setJobState(BJobState.STOPPED);
                } else {
                    jobRunInfo.setJobState(BJobState.COMPLETED);
                }

                BInternalLogger.debug(BJobContainer.class, "It have a job " +
                        "(" + job.getId() + ") that is over.");
            } catch (Exception e) {
                jobRunInfo.setJobState(BJobState.EXCEPTION);
                jobRunInfo.setCause(e);
            } finally {
                jobRunInfo.setEndTime(System.currentTimeMillis());
                //注销自己
                jobScheduler.removeContainer(jobRunInfo.getJobId());
                //发完成消息给主服务器,让心跳发送完成消息
                jobScheduler.notifyHaveCompleteMsg();
            }
        }

        void stop() {
            if (job != null) {
                job.stop();
            }
        }

    }

}
