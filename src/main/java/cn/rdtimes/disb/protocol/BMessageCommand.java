package cn.rdtimes.disb.protocol;


/**
 * 消息指令
 * Created by BZ on 2019/2/13.
 */
public final class BMessageCommand {

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
            default:
                return  "UNKNOWN";
        }
    }

    private BMessageCommand() {}

}
