package cn.rdtimes.disb.master;

import cn.rdtimes.disb.protocol.BReportJobMsg;

/**
 * 节点消息指令监听处理器
 * 用来执行指令
 * Created by BZ.
 */
interface IMessageListener {

    /**
     * 汇报任务指令通知
     *
     * @param reportJobMsg
     */
    void reportJob(String nodeHost, BReportJobMsg reportJobMsg);

    /**
     * 节点登录指令通知
     *
     * @param nodeHost
     * @param attachment 附件
     */
    void bind(String nodeHost, BProtocolHandler attachment);

    /**
     * 节点退出指令通知
     *
     * @param nodeHost   节点名称(如果此值为空,说明是节点有可能自行断开了)
     * @param attachment 附件
     */
    void unbind(String nodeHost, BProtocolHandler attachment);

}
