package cn.rdtimes.disb.core;

/**
 * 任务状态
 * Created by BZ.
 */
public enum BJobState {
    NONE,           //未知
    RUNNING,        //正在运行
    COMPLETED,      //已完成
    EXCEPTION,      //异常
    QUEUEING,       //队列中
    STOPPED,        //已停止
}
