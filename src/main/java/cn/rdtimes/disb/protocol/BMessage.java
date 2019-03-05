package cn.rdtimes.disb.protocol;

import cn.rdtimes.util.BBytesUtil;
import cn.rdtimes.util.BSerializerUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * 消息定义
 * Created by BZ on 2019/2/12.
 */
public class BMessage<T extends Serializable> {
    public final static int HEAD_SIZE = 20;

    private MsgHead head;       //消息头
    private T body;             //消息体

    public BMessage() {
        this(0, 0, null);
    }

    public BMessage(int command, int flag, T body) {
        head = new MsgHead();

        this.head.command = command;
        this.head.flag = flag;
        this.body = body;
    }

    public int getLength() {
        return head.length;
    }

    public int getCommand() {
        return head.command;
    }

    public void setCommand(int command) {
        this.head.command = command;
    }

    public int getFlag() {
        return head.flag;
    }

    public void setFlag(int flag) {
        this.head.flag = flag;
    }

    public long getTimes() {
        return head.times;
    }

    public long getSequence() {
        return head.sequence;
    }

    public T getBody() {
        return body;
    }

    public void setBody(T body) {
        this.body = body;
    }

    //组合消息到数组
    public byte[] combineMessage() throws IOException {
        byte[] bodyBytes = null;
        if (body != null) {  //序列化对象
            bodyBytes = BSerializerUtil.serialize(body);
        }

        head.length = (bodyBytes == null ? 0 : bodyBytes.length);
        byte[] msg = new byte[HEAD_SIZE + head.length];

        BBytesUtil.integerToBigEndianBytes(msg, 0, head.length);
        BBytesUtil.shortToBigEndianBytes(msg, 4, head.command);
        BBytesUtil.shortToBigEndianBytes(msg, 6, head.flag);
        head.times = System.currentTimeMillis();
        BBytesUtil.longToBigEndianBytes(msg, 8, head.times);
        head.sequence = getSeq();
        BBytesUtil.integerToBigEndianBytes(msg, 16, head.sequence);
        if (bodyBytes != null) {
            BBytesUtil.copyBytes(msg, HEAD_SIZE, bodyBytes.length, bodyBytes, 0);
        }

        return msg;
    }

    public String toString() {
        return "head:" + head + "; body:" + (body == null ? "" : body.toString());
    }

    //先分析出消息头部
    public static BMessage parseMessageHead(byte[] head) throws Exception {
        return parseMessageHead(head, 0);
    }

    public static BMessage parseMessageHead(byte[] head, int start) throws Exception {
        if (head == null || head.length == 0) {
            throw new IllegalArgumentException("head is null");
        }

        BMessage message = new BMessage();
        message.head.length = BBytesUtil.bigEndianBytesToInteger(head, start);
        message.head.command = (BBytesUtil.bigEndianBytesToShot(head, start + 4) & 0xFFFF);
        message.head.flag = (BBytesUtil.bigEndianBytesToShot(head, start + 6) & 0xFFFF);
        message.head.times = BBytesUtil.bigEndianBytesToLong(head, start + 8);
        message.head.sequence = BBytesUtil.bigEndianBytesToInteger(head, start + 16);
        return message;
    }

    public static BMessage parseMessageBody(byte[] body, BMessage message) throws Exception {
        return parseMessageBody(body, 0, message);
    }

    //分析出消息体,BIND_RESP无消息体
    public static BMessage parseMessageBody(byte[] body, int start, BMessage message) throws Exception {
        if (body == null || body.length == 0) return message;

        byte[] copyBody = (start <= 0 ? body : Arrays.copyOfRange(body, start, body.length));

        if (message.head.command == BMessageCommand.BIND) {
            BBindMsg bindMsg = BSerializerUtil.deserialize(copyBody);
            message.setBody(bindMsg);
        } else if (message.head.command == BMessageCommand.REPORT_HEARTBEAT) {
            BReportJobMsg reportJobMsg = BSerializerUtil.deserialize(copyBody);
            message.setBody(reportJobMsg);
        } else if (message.head.command == BMessageCommand.START_JOB) {
            BStartJobMsg startJobMsg = BSerializerUtil.deserialize(copyBody);
            message.setBody(startJobMsg);
        } else if (message.head.command == BMessageCommand.STOP_JOB) {
            BStopJobMsg stopJobMsg = BSerializerUtil.deserialize(copyBody);
            message.setBody(stopJobMsg);
        } else if (message.head.command == BMessageCommand.UNBIND) {
            BUnbindMsg unbindMsg = BSerializerUtil.deserialize(copyBody);
            message.setBody(unbindMsg);
        }

        return message;
    }

    private static int SEQ_ = 0;

    public synchronized static int getSeq() {
        if (SEQ_ == Integer.MAX_VALUE) {
            SEQ_ = 0;
        }
        return ++SEQ_;
    }

    final class MsgHead {
        private int length;         //长度
        private int command;        //指令
        private int flag;           //标志
        private long times;         //时间戳
        private int sequence;       //流水号

        public String toString() {
            return "length:" + length + "; command:" + command + "[" + BMessageCommand.ToString(command) +
                    "]; flag:" + flag + "; times:" + times + "; sequence:" + sequence;
        }
    }

}
