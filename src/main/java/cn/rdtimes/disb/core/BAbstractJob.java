package cn.rdtimes.disb.core;


/**
 * 抽象任务类
 * Created by BZ.
 */
public abstract class BAbstractJob implements IJob {
    protected BSplit split;
    protected String jobId;

    public String getId() {
        return jobId;
    }

    public void setId(String id) {
        this.jobId = id;
    }

    public void setSplit(BSplit split) {
        if (this.split != null) {
            throw new IllegalStateException("split is only once assigned");
        }
        this.split = split;
    }

    public BSplit getSplit() {
        return split;
    }

    public void stop() {}

}
