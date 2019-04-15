package cn.rdtimes.disb.protocol;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 汇报任务状态消息体
 * 如果没有任务可以把当做为心跳测试.
 * <p>
 * Created by BZ.
 */
public class BReportJobMsg implements Serializable {
    private final static long serialVersionUID = -1;

    //任务数量,如果为0将视为心跳测试消息
    private int jobCount;
    //节点名称
    private String nodeName;
    //任务信息列表
    private List<BJobInfoMsg> jobInfoMsgs;

    public BReportJobMsg() {
    }

    public int getJobCount() {
        return jobCount;
    }

    public void setJobCount(int jobCount) {
        this.jobCount = jobCount;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public List<BJobInfoMsg> getJobInfoMsgs() {
        return jobInfoMsgs;
    }

    public void add(BJobInfoMsg jobInfoMsg) {
        if (jobInfoMsgs == null) {
            jobInfoMsgs = new ArrayList();
        }

        if (!jobInfoMsgs.contains(jobInfoMsg)) {
            jobInfoMsgs.add(jobInfoMsg);
        }
    }

    public void remove(BJobInfoMsg jobInfoMsg) {
        if (jobInfoMsgs != null) {
            jobInfoMsgs.remove(jobInfoMsg);
        }
    }

    public BJobInfoMsg getJobInfoMsg(String jobId) {
        if (jobInfoMsgs == null || jobInfoMsgs.size() == 0) return null;

        for (BJobInfoMsg jobInfoMsg : jobInfoMsgs) {
            if (jobInfoMsg.getJobId().equals(jobId)) {
                return jobInfoMsg;
            }
        }

        return null;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(32);
        sb.append("jobCount:").append(jobCount).append(";");
        sb.append(" nodeName:").append(nodeName);
        if (jobInfoMsgs != null) {
            sb.append("; msgCount:" + jobInfoMsgs.size()).append(" [");
            for (int i = 0; i < jobInfoMsgs.size(); i++) {
                BJobInfoMsg jobInfoMsg = jobInfoMsgs.get(i);
                sb.append(jobInfoMsg);
                if ((i + 1) != jobInfoMsgs.size()) {
                    sb.append("|");
                }
            }
            sb.append("]");
        }

        return sb.toString();
    }

}
