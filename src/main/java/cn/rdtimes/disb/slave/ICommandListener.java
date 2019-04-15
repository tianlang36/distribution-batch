package cn.rdtimes.disb.slave;

import cn.rdtimes.disb.protocol.BStartJobMsg;
import cn.rdtimes.disb.protocol.BStopJobMsg;

/**
 * 执行消息指令
 * 命令模式
 * Created by BZ.
 */
interface ICommandListener {

    /**
     * 启动指定任务指令通知
     *
     * @param startJobMsg
     */
    void startJob(BStartJobMsg startJobMsg);

    /**
     * 停止指定任务指令通知
     *
     * @param stopJobMsg
     */
    void stopJob(BStopJobMsg stopJobMsg);

}
