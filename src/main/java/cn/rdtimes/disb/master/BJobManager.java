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
 * 1.暂时没有使用队列提交任务(应该控制并发)

 * Created by BZ.
 */
class BJobManager {
    //正在运行job的集合,key=jobId,value=节点列表等信息
    private final Map<String, List<RunningJobInfo>> runningJobMap;
    //正在运行job的future对象,与上面的对应
    private final Map<String, RunningJobFuture> runningJobFutureMap;

    BJobManager() {
        runningJobMap = new ConcurrentHashMap<String, List<RunningJobInfo>>(1);
        runningJobFutureMap = new ConcurrentHashMap<String, RunningJobFuture>(1);
    }

    private static int _SEQ = 1;

    //返回新的任务编号
    private synchronized static String getJobId() {
        if (_SEQ > 100000) { //最大任务编号数目
            _SEQ = 1;
        }

        SimpleDateFormat format0 = new SimpleDateFormat("yyyyMMdd");
        String jobId = format0.format(new Date());
        jobId += "-" + String.format("%06d", _SEQ);
        ++_SEQ;

        return jobId;
    }

    //////////////////IJobManager/////////////////

    //启动任务,是否考虑使用任务队列来管理
    public BClientStartJobRespMsg startJob(String jobClassName, String inputSplitClassName) {
        //1.随机生成一个jobId
        String jobId = getJobId();

        BClientStartJobRespMsg startJobRespMsg = new BClientStartJobRespMsg();
        startJobRespMsg.setJobId(jobId);
        startJobRespMsg.setJobState(BJobState.RUNNING);

        if (BStringUtil.isEmpty(jobClassName) || BStringUtil.isEmpty((inputSplitClassName))) {
            startJobRespMsg.setJobState(BJobState.EXCEPTION);
            startJobRespMsg.setCause(new IllegalArgumentException("jobClassName or " +
                    "inputSplitClassName is null"));
            return startJobRespMsg;
        }

        String[] hostNames;
        try {
            //2.创建分片器
            int activeNodeCount = BMasterService.getNioServer().getActiveNodeCount();
            IInputSplit inputSplit = createInputSplit(inputSplitClassName);
            BSplit[] splits = inputSplit.getSplit(activeNodeCount);
            if (splits == null || splits.length == 0 || splits.length > activeNodeCount) {
                startJobRespMsg.setJobState(BJobState.EXCEPTION);
                startJobRespMsg.setCause(new IllegalArgumentException("split need to more than " +
                        "count of activity node"));
                return startJobRespMsg;
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
            hostNames = BMasterService.getNioServer().writeStartJobMessage(messages);
            startJobRespMsg.setNodeCount(hostNames.length);
            startJobRespMsg.setStartTime(System.currentTimeMillis());
        } catch (Exception e) {
            startJobRespMsg.setJobState(BJobState.EXCEPTION);
            startJobRespMsg.setCause(e);
            return startJobRespMsg;
        }

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
            runningJobFutureMap.put(jobId, runningJobFuture);
        }

        return startJobRespMsg;
    }

    private IInputSplit createInputSplit(String inputSplitClassName) throws Exception {
        Class clazz = Class.forName(inputSplitClassName);
        return (IInputSplit) clazz.newInstance();
    }

    //停止任务
    public void stopJob(String id) throws Exception {
        if (!runningJobMap.containsKey(id)) {
            throw new IllegalStateException(id + " is not a running job");
        }

        RunningJobFuture runningJobFuture = runningJobFutureMap.get(id);
        runningJobFuture.stopFlag = true;

        //发送停止消息
        BStopJobMsg stopJobMsg = new BStopJobMsg();
        stopJobMsg.setJobId(id);
        BMasterService.getNioServer().writeStopJobMessage(new BMessage<BStopJobMsg>(BMessageCommand.STOP_JOB,
                0, stopJobMsg), runningJobFuture.nodeNames);
    }

    //节点消息状态报告通知
    void jobReportNotify(String nodeHost, BReportJobMsg reportJobMsg) {
        //没有运行的任务就返回
        if (runningJobMap.size() == 0 || stopFlag) return;

        //更新正在运行任务对应节点的状态
        if (reportJobMsg.getJobCount() > 0) {
            List<BJobInfoMsg> jobInfoMsgs = reportJobMsg.getJobInfoMsgs();
            for (BJobInfoMsg jobInfoMsg : jobInfoMsgs) {
                String jobId = jobInfoMsg.getJobId();
                updateRunningJobMap(jobId, reportJobMsg.getNodeName(), jobInfoMsg);
            }
        } else { //是心跳,有一种情况就是节点没有成功发送正在运行任务状态(可能断网了)
            updateRunningJobMapByNodeName(nodeHost);
        }

        synchronized (lock) { //通知检查线程工作
            lock.notify();
        }
    }

    //节点状态报告通知时更新相关节点下的状态信息
    private void updateRunningJobMap(String jobId, String nodeHost, BJobInfoMsg jobInfoMsg) {
        synchronized (this) {
            List<RunningJobInfo> runningJobInfoList = runningJobMap.get(jobId);
            if (runningJobInfoList == null || runningJobInfoList.size() == 0) return;

            for (RunningJobInfo jobInfo : runningJobInfoList) {
                if (jobInfo.nodeName.equals(nodeHost)) {
                    jobInfo.lastAccessedTime = System.currentTimeMillis();
                    jobInfo.failCount = 0;
                    jobInfo.jobInfoMsg = jobInfoMsg;

                    return;
                }
            }
        }
    }

    //节点状态报告通知根据节点更新(非正常状态报告的,但任务状态未知)
    private void updateRunningJobMapByNodeName(String nodeName) {
        synchronized (this) {
            Set<Map.Entry<String, List<RunningJobInfo>>> entries = runningJobMap.entrySet();
            for (Map.Entry<String, List<RunningJobInfo>> entry : entries) {
                List<RunningJobInfo> runningJobInfoList = entry.getValue();
                for (RunningJobInfo runningJobInfo : runningJobInfoList) {
                    if (runningJobInfo.nodeName.equals(nodeName)) {
                        //看看是否达到断网后计数值
                        if (runningJobInfo.failCount < BMasterConf.getInstance().getJobCompleteFailCount()) {
                            runningJobInfo.failCount++;
                            break;
                        }

                        if (runningJobInfo.jobInfoMsg == null) {
                            runningJobInfo.jobInfoMsg = new BJobInfoMsg();
                            runningJobInfo.jobInfoMsg.setJobId(entry.getKey());
                        }

                        //修改其状态为异常,无法确定是否完成
                        if (runningJobInfo.jobInfoMsg.getState() != BJobState.COMPLETED) {
                            runningJobInfo.jobInfoMsg.setState(BJobState.EXCEPTION);
                            runningJobInfo.jobInfoMsg.setCause(new Error("unknown job state because of " +
                                    "channel(" + nodeName + ") is closed"));
                        }
                    }
                } //end for
            } //end for
        } //end syn
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

        private void checkTimeout() {  //这个地方可以考虑其他算法,如果数量大会使同步时间较长
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
                        }
                    } catch (Exception e) {
                        BInternalLogger.error(BJobManager.class, "check report error:", e);
                    } finally {
                        //根据jobId获取每个任务状态的情况,完成的任务将通知并删除
                        BJobReport jobReport = getJobReport(jobId);
                        if (jobReport.getTotalCount() > 0 && jobReport.equalTotalCount()) {
                            //任务都执行完成发送关闭消息
                            sendClientCloseMessage(jobReport);
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

    private void sendClientCloseMessage(BJobReport jobReport) {
        String jobId = jobReport.getJobId();

        BClientCloseJobMsg clientCloseJobMsg = new BClientCloseJobMsg();
        clientCloseJobMsg.setJobId(jobId);
        clientCloseJobMsg.setTotalCount(jobReport.getTotalCount());
        clientCloseJobMsg.setCanceledCount(jobReport.getCanceledCount());
        clientCloseJobMsg.setFailureCount(jobReport.getFailureCount());
        clientCloseJobMsg.setSuccessCount(jobReport.getSuccessCount());
        clientCloseJobMsg.setCause(jobReport.getCause());

        BMessage<BClientCloseJobMsg> message = new BMessage<BClientCloseJobMsg>(BMessageCommand
                .CLIENT_CLOSE, 0, clientCloseJobMsg);
        try {
            BMasterService.getClientNioServer().writeMessage(jobId, message);
            BMasterService.getClientNioServer().removeAndCloseChannel(jobId);
        } catch (Exception e) {
            BInternalLogger.error(BJobManager.class, "send client message error:", e);
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
        int failCount;           //当节点丢失时的计数,当计数达到一定数量时就认为认为结束
    }

    private class RunningJobFuture {
        String[] nodeNames;
        boolean stopFlag;  //是否调用了停止方法
    }

}
