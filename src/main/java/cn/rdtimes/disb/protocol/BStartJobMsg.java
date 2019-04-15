package cn.rdtimes.disb.protocol;

import cn.rdtimes.disb.core.BSplit;

import java.io.Serializable;

/**
 * 启动任务消息体
 * Created by BZ.
 */
public class BStartJobMsg implements Serializable {
    private final static long serialVersionUID = -1;

    //任务编号
    private String jobId;
    //任务类全限名称
    private String jobClassName;
    //分片信息
    private BSplit split;

    public BStartJobMsg() {}

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public BSplit getSplit() {
        return split;
    }

    public void setSplit(BSplit split) {
        this.split = split;
    }

    public String getJobClassName() {
        return jobClassName;
    }

    public void setJobClassName(String jobClassName) {
        this.jobClassName = jobClassName;
    }

    public String toString() {
        return "jobId:" + jobId + " ;jobClassName:" + jobClassName + "; split:" + split;
    }

}
