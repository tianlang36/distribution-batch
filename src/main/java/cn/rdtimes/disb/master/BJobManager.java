package cn.rdtimes.disb.master;

import cn.rdtimes.disb.core.*;
import cn.rdtimes.disb.protocol.*;
import cn.rdtimes.util.BStringUtil;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * 任务管理器实现
 * 1.暂时只能提交一个任务
 * 2.任务状态更新有几种情况
 * 1)节点正常发送状态报告
 * 2)节点开始发心跳,但未发任务状态报告(可能断网),由于任务没有完成,后来正常发状态报告
 * 3)节点开始发心跳,但未发任务状态报告(可能断网),由于任务完成,后来正常发心跳
 * 4)节点开始发心跳,正常发任务状态报告,但任务未完成不能正常发状态报告,后来恢复可能发状态报告也可能发心跳
 * 5)节点开始发心跳,任务执行后一直断网,未发任何报告或心跳
 * Created by BZ on 2019/2/19.
 */
class BJobManager implements IJobManager {
    //正在运行job的集合,key=jobId,value=节点列表等信息
    private final Map<String, List<RunningJobInfo>> runningJobMap;
    //正在运行job的future对象,与上面的对应
    private final Map<String, RunningJobFuture> runningJobFutureMap;

    private IJobReportListener jobReportListener;

    BJobManager() {
        runningJobMap = new ConcurrentHashMap<String, List<RunningJobInfo>>(1);
        runningJobFutureMap = new ConcurrentHashMap<String, RunningJobFuture>(1);
    }

    private static int _SEQ = 1;

    //返回新的任务编号
    private synchronized static String getJobId() {
        if (_SEQ > 10000) { //最大任务编号数目
            _SEQ = 1;
        }

        SimpleDateFormat format0 = new SimpleDateFormat("yyyyMMdd");
        String jobId = format0.format(new Date());
        jobId += "-" + String.format("%05d", _SEQ);
        ++_SEQ;

        return jobId;
    }

    //////////////////IJobManager/////////////////

    //启动任务
    public IJobFuture startJob(String jobClassName, String inputSplitClassName) throws Exception {
        if (runningJobMap.size() > 0) { //下个版本支持多个任务
            throw new IllegalArgumentException("only a job");
        }

        if (BStringUtil.isEmpty(jobClassName) || BStringUtil.isEmpty((inputSplitClassName))) {
            throw new IllegalArgumentException("jobClassName or inputSplitClassName is null");
        }

        //1.随机生成一个jobId
        String jobId = getJobId();
        IJobFuture jobFuture = new BDefaultJobFuture(jobId);

        //2.创建分片器
        int activeNodeCount = BMasterService.getNioServer().getActiveNodeCount();
        IInputSplit inputSplit = createInputSplit(inputSplitClassName);
        BSplit[] splits = inputSplit.getSplit(activeNodeCount);
        if (splits == null || splits.length == 0 || splits.length > activeNodeCount) {
            throw new IllegalArgumentException("split need to more than count of activity node");
        }

        BInternalLogger.debug(BJobManager.class, "Job (" + jobId + ")" +
                " have been split that is size " + splits.length);

        //3.根据分片生成启动任务消息
        int i = 0;
        BMessage[] messages = new BMessage[splits.length];
        for (BSplit split : splits) {
            BStartJobMsg startJobMsg = new BStartJobMsg();
            startJobMsg.setJobId(jobId);
            startJobMsg.setJobClassName(jobClassName);
            startJobMsg.setSplit(split);

            BMessage<BStartJobMsg> message = new BMessage<BStartJobMsg>(BMessageCommand.START_JOB,
                    0, startJobMsg);
            messages[i] = message;
            ++i;
        }

        //4.写启动消息
        String[] hostNames = BMasterService.getNioServer().writeStartJobMessage(messages);

        //5.更新状态
        synchronized (this) { //需要同步
            List<RunningJobInfo> runningJobInfoList = new ArrayList<RunningJobInfo>(hostNames.length);
            for (String name : hostNames) {
                RunningJobInfo runningJobInfo = new RunningJobInfo();
                runningJobInfo.nodeName = name;

                runningJobInfoList.add(runningJobInfo);
            }
            runningJobMap.put(jobId, runningJobInfoList);

            RunningJobFuture runningJobFuture = new RunningJobFuture();
            runningJobFuture.nodeNames = hostNames;
            runningJobFuture.startJobFuture = jobFuture;
            runningJobFutureMap.put(jobId, runningJobFuture);
        }

        return jobFuture;
    }

    private IInputSplit createInputSplit(String inputSplitClassName) throws Exception {
        Class clazz = Class.forName(inputSplitClassName);
        return (IInputSplit) clazz.newInstance();
    }

    //停止任务
    public IJobFuture stopJob(String id) throws Exception {
        if (runningJobMap.containsKey(id)) {
            throw new IllegalStateException(id + " is not a running job");
        }

        RunningJobFuture runningJobFuture = runningJobFutureMap.get(id);
        runningJobFuture.stopFlag = true;
        //通知停止在startJob中的future
        runningJobFuture.startJobFuture.setCompleted(null);
        runningJobFuture.stopJobFuture = new BDefaultJobFuture(id);
        //发送停止消息
        BStopJobMsg stopJobMsg = new BStopJobMsg();
        stopJobMsg.setJobId(id);
        BMasterService.getNioServer().writeStopJobMessage(new BMessage<BStopJobMsg>(BMessageCommand.STOP_JOB,
                0, stopJobMsg), runningJobFuture.nodeNames);

        return runningJobFuture.stopJobFuture;
    }

    public void setJobReportListener(IJobReportListener jobReportListener) {
        this.jobReportListener = jobReportListener;
    }

    //消息状态报告通知
    void jobReportNotify(String nodeHost, BReportJobMsg reportJobMsg) {
        if (jobReportListener == null) { //如果为空,使用缺省的对象
            jobReportListener = new BJobManager.BDefaultJobReportListener();
        }

        //没有运行的任务就返回
        if (runningJobMap.size() == 0 || stopFlag) return;

        //更新正在运行任务对应节点的状态
        if (reportJobMsg.getJobCount() > 0) {
            List<BJobInfoMsg> jobInfoMsgs = reportJobMsg.getJobInfoMsgs();
            for (BJobInfoMsg jobInfoMsg : jobInfoMsgs) {
                String jobId = jobInfoMsg.getJobId();
                updateRunningJobMap(jobId, reportJobMsg.getNodeName(), jobInfoMsg);
                jobReportListener.jobReport(jobId, getJobReport(jobId));
            }
        } else { //是心跳,有一种情况就是节点没有成功发送正在运行任务状态(可能断网了)
            updateRunningJobMapByNodeName(nodeHost);
        }

        synchronized (lock) { //通知检查线程工作
            lock.notify();
        }
    }

    //状态报告通知时更新相关节点下的状态信息
    private void updateRunningJobMap(String jobId, String nodeHost, BJobInfoMsg jobInfoMsg) {
        synchronized (this) {
            List<RunningJobInfo> runningJobInfoList = runningJobMap.get(jobId);
            if (runningJobInfoList == null || runningJobInfoList.size() == 0) return;

            for (RunningJobInfo jobInfo : runningJobInfoList) {
                if (jobInfo.nodeName.equals(nodeHost)) {
                    jobInfo.lastAccessedTime = System.currentTimeMillis();
                    jobInfo.jobInfoMsg = jobInfoMsg;

                    return;
                }
            }
        }
    }

    //状态报告通知根据节点更新(非正常状态报告的,但任务未结束)
    private void updateRunningJobMapByNodeName(String nodeName) {
        synchronized (this) {
            Set<Map.Entry<String, List<RunningJobInfo>>> entries = runningJobMap.entrySet();
            for (Map.Entry<String, List<RunningJobInfo>> entry : entries) {
                List<RunningJobInfo> runningJobInfoList = entry.getValue();
                for (RunningJobInfo runningJobInfo : runningJobInfoList) {
                    if (runningJobInfo.nodeName.equals(nodeName)) {
                        //看看是否超过丢失节点的时间戳
                        long interval = System.currentTimeMillis() - runningJobInfo.lastAccessedTime;
                        if (interval < BMasterConf.getInstance().getMissingNodeMaxTime()) {
                            break;
                        }

                        if (runningJobInfo.jobInfoMsg == null) {
                            runningJobInfo.jobInfoMsg = new BJobInfoMsg();
                            runningJobInfo.jobInfoMsg.setJobId(entry.getKey());
                            runningJobInfo.jobInfoMsg.setState(BJobState.EXCEPTION);
                            runningJobInfo.jobInfoMsg.setCause(new TimeoutException("interval " +
                                    "is more " + interval + "ms"));
                        }

                        if (runningJobInfo.jobInfoMsg.getState() == BJobState.QUEUEING ||
                                runningJobInfo.jobInfoMsg.getState() == BJobState.RUNNING) {
                            runningJobInfo.jobInfoMsg.setState(BJobState.COMPLETED);
                        }

                        break;
                    }
                }
            }
        }
    }

    //根据任务id汇总状态报告
    private BJobReport getJobReport(String jobId) {
        BJobReport jobReport = new BJobReport();
        jobReport.setJobId(jobId);

        List<RunningJobInfo> runningJobInfoList = runningJobMap.get(jobId);
        if (runningJobInfoList == null || runningJobInfoList.size() == 0) {
            return jobReport;
        }

        jobReport.setTotalCount(runningJobInfoList.size());
        for (RunningJobInfo runningJobInfo : runningJobInfoList) {
            if (runningJobInfo.jobInfoMsg == null) continue;

            if (runningJobInfo.jobInfoMsg.getState() == BJobState.COMPLETED ||
                    runningJobInfo.jobInfoMsg.getState() == BJobState.NONE) {
                jobReport.setSuccessCount(jobReport.getSuccessCount() + 1);
                jobReport.setRunningCount(jobReport.getRunningCount() - 1);
            } else if (runningJobInfo.jobInfoMsg.getState() == BJobState.STOPPED) {
                jobReport.setCanceledCount(jobReport.getCanceledCount() + 1);
                jobReport.setRunningCount(jobReport.getRunningCount() - 1);
            } else if (runningJobInfo.jobInfoMsg.getState() == BJobState.EXCEPTION) {
                jobReport.setFailureCount(jobReport.getFailureCount() + 1);
                jobReport.setRunningCount(jobReport.getRunningCount() - 1);

                if (runningJobInfo.jobInfoMsg.getCause() != null) {
                    String cause = jobReport.getCause();
                    if (!BStringUtil.isEmpty(cause)) cause += "|";
                    else cause = "";
                    cause += runningJobInfo.jobInfoMsg.getCause().getMessage();
                    jobReport.setCause(cause);
                }
            } else {
                jobReport.setRunningCount(jobReport.getRunningCount() + 1);
            }
        }

        return jobReport;
    }

    void start() {
        CheckThread checkThread = new CheckThread();
        checkThread.start();
    }

    void shutdown() {
        stopFlag = true;
        synchronized (lock) {
            lock.notify();
        }
    }

    //缺省实现(如果使用消息需要重新实现),向客户端汇报任务状态
    private class BDefaultJobReportListener implements IJobReportListener {

        public void jobReport(String jobId, BJobReport jobReport) {
            BInternalLogger.info(BJobManager.class, jobReport.toString());
        }

    }

    private volatile boolean stopFlag;
    private final Object lock = new Object();

    //检查有哪些任务对应的节点超过指定时间未汇报状态了
    //超时了就算完成
    //任务对应所有节点都完成了就通知客户端
    private class CheckThread extends Thread {
        private final static int interval = 3000;

        CheckThread() {
            super("Master-CheckJobReportThread");
        }

        public void run() {
            BInternalLogger.info(BJobManager.class, getName() + " have been started.");

            while (!stopFlag) {
                if (runningJobMap.size() > 0) {
                    checkTimeout();
                }

                synchronized (lock) {
                    try {
                        lock.wait(interval);
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
            }

            BInternalLogger.info(BJobManager.class, getName() + " is over.");
        }

        private void checkTimeout() {
            synchronized (BJobManager.this) {
                String jobId;

                Set<Map.Entry<String, List<RunningJobInfo>>> entrySet = runningJobMap.entrySet();
                for (Map.Entry<String, List<RunningJobInfo>> entry : entrySet) {
                    jobId = entry.getKey();
                    try {
                        List<RunningJobInfo> runningJobInfoList = entry.getValue();
                        if (runningJobInfoList == null || runningJobInfoList.size() == 0) {
                            continue;
                        }

                        for (RunningJobInfo runningJobInfo : runningJobInfoList) {
                            //更新状态,看看是否超过丢失节点的时间戳
                            long interval = System.currentTimeMillis() - runningJobInfo.lastAccessedTime;
                            if (interval >= BMasterConf.getInstance().getMissingNodeMaxTime()) {
                                //有可能为空,自从运行一直都在断网,未发任务报告或心跳
                                if (runningJobInfo.jobInfoMsg == null) {
                                    runningJobInfo.jobInfoMsg = new BJobInfoMsg();
                                    runningJobInfo.jobInfoMsg.setJobId(entry.getKey());
                                }
                                runningJobInfo.jobInfoMsg.setState(BJobState.EXCEPTION);
                                runningJobInfo.jobInfoMsg.setCause(new TimeoutException("interval " +
                                        "is more " + interval + "ms"));
                            }

                            if (runningJobInfo.jobInfoMsg == null) continue;

                            if (runningJobInfo.jobInfoMsg.getState() == BJobState.EXCEPTION ||
                                    runningJobInfo.jobInfoMsg.getState() == BJobState.NONE ||
                                    runningJobInfo.jobInfoMsg.getState() == BJobState.STOPPED ||
                                    runningJobInfo.jobInfoMsg.getState() == BJobState.COMPLETED) {
                                continue;
                            }
                        }
                    } catch (Exception e) {
                        BInternalLogger.error(BJobManager.class, "check report error:", e);
                    } finally {
                        //根据jobId获取每个任务状态的情况,完成的任务将通知并删除
                        BJobReport jobReport = getJobReport(jobId);
                        if (jobReport.getTotalCount() > 0 && jobReport.equalTotalCount()) { //任务都执行完成
                            RunningJobFuture jobFuture = runningJobFutureMap.get(jobId);
                            if (jobFuture.stopFlag) {
                                jobFuture.stopJobFuture.setCompleted(jobReport);
                            } else {
                                jobFuture.startJobFuture.setCompleted(jobReport);
                                if (jobFuture.stopJobFuture != null) {
                                    jobFuture.stopJobFuture.setCompleted(jobReport);
                                }
                            }

                            //删除任务
                            removeJob(jobId);

                            BInternalLogger.debug(BJobManager.class, "It have a job completion [" +
                                    jobReport + "]");
                        }
                    }
                } //end for
            } //end synchronized
        }

    }

    private void removeJob(String jobId) {
        runningJobMap.remove(jobId);
        runningJobFutureMap.remove(jobId);
    }

    private class RunningJobInfo {
        String nodeName;
        long lastAccessedTime = System.currentTimeMillis();
        BJobInfoMsg jobInfoMsg;  //当有状态汇报或丢失节点超时时会赋值
    }

    private class RunningJobFuture {
        IJobFuture startJobFuture;
        IJobFuture stopJobFuture;
        String[] nodeNames;
        boolean stopFlag;  //是否调用了停止方法
    }

}
