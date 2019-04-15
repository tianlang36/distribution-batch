package cn.rdtimes.disb.core;


import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 缺省实现
 * Created by BZ.
 */
public class BDefaultJobFuture implements IJobFuture {
    //任务编号
    private final String jobId;

    //等待者数量
    private int waiters;
    //是否完成
    private boolean done;

    public BDefaultJobFuture(String jobId) {
        this.jobId = jobId;
    }

    public String getId() {
        return jobId;
    }

    public BJobReport waitCompleted() {
        await0();
        return jobReport;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return setCompleted(jobReport);
    }

    public boolean isCancelled() {
        return false;
    }

    public synchronized boolean isDone() {
        return done;
    }

    private BJobReport jobReport;

    public BJobReport get() throws InterruptedException, ExecutionException {
        await0();
        return jobReport;
    }

    public BJobReport get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        return get();
    }

    //无中断处理
    private void await0() {
        boolean interrupted = false;
        synchronized (this) {
            while (!done) {
                waiters++;
                try {
                    wait();
                } catch (InterruptedException ie) {
                    interrupted = true;
                } finally {
                    waiters--;
                }
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean setCompleted(BJobReport jobReport) {
        synchronized (this) {
            if (done) {
                return false;
            }
            done = true;
            if (waiters > 0) {
                notifyAll();
            }

            this.jobReport = jobReport;
        }

        return true;
    }

}
