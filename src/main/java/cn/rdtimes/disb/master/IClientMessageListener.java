package cn.rdtimes.disb.master;


import cn.rdtimes.disb.protocol.BClientStartJobMsg;

/**
 * 节点消息指令监听处理器
 * 用来执行指令
 * Created by BZ.
 */
interface IClientMessageListener {

    /**
     * 汇报任务指令通知
     *
     * @param attachment
     */
    void startJob(BClientStartJobMsg startJobMsg, BClientProtocolHandler attachment);

    /**
     * 节点登录指令通知
     *
     * @param jobId
     * @param attachment 附件
     */
    void stopJob(String jobId, BClientProtocolHandler attachment);

    /**
     * 节点被关闭通知（断开）
     *
     * @param attachment 附件
     */
    void close(BClientProtocolHandler attachment);

}
