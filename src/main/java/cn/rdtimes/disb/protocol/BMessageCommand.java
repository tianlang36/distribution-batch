package cn.rdtimes.disb.protocol;


/**
 * 消息指令
 * Created by BZ.
 */
public final class BMessageCommand {

    //////节点端指令///////
    //登录指令
    public final static int BIND = 0x1;
    //登录响应指令
    public final static int BIND_RESP = 0x8001;
    //退出指令
    public final static int UNBIND = 0x2;
    //汇报或心跳
    public final static int REPORT_HEARTBEAT = 0x3;
    //启动任务
    public final static int START_JOB = 0x4;
    //停止任务
    public final static int STOP_JOB = 0x5;

    //////客户端指令///////
    //启动任务指令
    public final static int CLIENT_START_JOB = 0x81;
    //启动任务响应指令
    public final static int CLIENT_START_JOB_RESP = 0x8081;
    //关闭客户端指令
    public final static int CLIENT_CLOSE = 0x82;
    //停止任务指令
    public final static int CLIENT_STOP_JOB = 0x83;
    //心跳测试或状态汇报
    public final static int CLIENT_REPORT_HEARTBEAT = 0x84;

    public static String ToString(int command) {
        switch (command) {
            case BIND:
                return "BIND";
            case BIND_RESP:
                return "BIND_RESP";
            case UNBIND:
                return "UNBIND";
            case REPORT_HEARTBEAT:
                return "REPORT_HEARTBEAT";
            case START_JOB:
                return "START_JOB";
            case STOP_JOB:
                return "STOP_JOB";
            case CLIENT_START_JOB:
                return "CLIENT_START_JOB";
            case CLIENT_START_JOB_RESP:
                return "CLIENT_START_JOB_RESP";
            case CLIENT_CLOSE:
                return "CLIENT_CLOSE";
            case CLIENT_STOP_JOB:
                return "CLIENT_STOP_JOB";
            case CLIENT_REPORT_HEARTBEAT:
                return "CLIENT_REPORT_HEARTBEAT";
            default:
                return  "UNKNOWN";
        }
    }

    private BMessageCommand() {}

}
