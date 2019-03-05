package cn.rdtimes.disb.core;

import java.util.concurrent.Future;

/**
 * 提交任务后返回的future接口
 * Created by BZ on 2019/2/19.
 */
public interface IJobFuture extends Future<cn.rdtimes.disb.core.BJobReport> {

    /**
     * 返回job编号
     * @return
     */
    String getId();

    /**
     * 等待job结束,并返回所有任务的报告
     * @return 所有任务的报告
     * @throws InterruptedException
     */
    BJobReport waitCompleted();

    /**
     * 设置完成
     * @param jobReport 所有任务的报告
     * @return true成功,否则如果调用过或已经结束
     */
    boolean setCompleted(BJobReport jobReport);


}
