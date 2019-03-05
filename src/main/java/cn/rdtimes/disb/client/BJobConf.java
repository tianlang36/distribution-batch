package cn.rdtimes.disb.client;

/**
 * 客户端任务配置
 * Created by BZ on 2019/2/13.
 */
public class BJobConf {
    private String jobClassName;
    private String inputSplitClassName;

    public BJobConf() {}

    public String getInputSplitClassName() {
        return inputSplitClassName;
    }

    public void setInputSplitClassName(String inputSplitClassName) {
        this.inputSplitClassName = inputSplitClassName;
    }

    public String getJobClassName() {
        return jobClassName;
    }

    public void setJobClassName(String jobClassName) {
        this.jobClassName = jobClassName;
    }

}
