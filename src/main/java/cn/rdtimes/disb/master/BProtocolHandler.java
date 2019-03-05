package cn.rdtimes.disb.master;

import cn.rdtimes.disb.core.BInternalLogger;
import cn.rdtimes.disb.protocol.*;
import cn.rdtimes.nio.lf.BAsynchChannel;
import cn.rdtimes.nio.lf.BFrameAsynchHandler;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * 协议处理
 * Created by BZ on 2019/2/19.
 */
class BProtocolHandler extends BFrameAsynchHandler {
    private final IMessageListener messageListener;

    BProtocolHandler(BProtocolHandlerFactory f, IMessageListener messageListener) {
        super(f);
        this.messageListener = messageListener;
    }

    public void channelClosed(BAsynchChannel channel) throws Exception {
        messageListener.unbind(null, this);
        super.channelClosed(channel);
    }

    protected Object decode(BAsynchChannel channel, ByteBuffer buffer) {
        //0.判断是否满足头部长度
        int bufferMsgLen = buffer.remaining();
        if (bufferMsgLen < BMessage.HEAD_SIZE) {
            return null;
        } else {
            //1.先获取一下消息体长度和消息总长度
            buffer.order(ByteOrder.BIG_ENDIAN);
            int msgTotalLen = BMessage.HEAD_SIZE + buffer.getInt(0);
            //2.先判断消息的总长度是否满足
            if (msgTotalLen <= bufferMsgLen) { //满足
                try {
                    byte[] arrayData = buffer.array();
                    //拷贝一个帧的数据
                    byte[] msgFrame = Arrays.copyOfRange(arrayData, 0, msgTotalLen);
                    return ByteBuffer.wrap(msgFrame);
                } finally { //最后要保证最开始的位置
                    buffer.position(buffer.position() + msgTotalLen);
                }
            } else { //继续读取消息内容
                return null;
            }
        }
    }

    /**
     * 解析成相应协议
     *
     * @param data 协议一帧数据
     * @throws Exception
     */
    protected void process(Object data) throws Exception {
        ByteBuffer dataBuffer = (ByteBuffer) data;
        byte[] adata = dataBuffer.array();
        BMessage message = BMessage.parseMessageHead(adata);
        message = BMessage.parseMessageBody(adata, BMessage.HEAD_SIZE, message);

        if (message.getCommand() == BMessageCommand.BIND) {
            //发送登录响应消息
            writeMsgBindResp();
            //通知bind
            messageListener.bind(((BBindMsg) message.getBody()).getNodeName(), this);
        } else if (message.getCommand() == BMessageCommand.UNBIND) {
            messageListener.unbind(((BUnbindMsg) message.getBody()).getNodeName(), this);
        } else if (message.getCommand() == BMessageCommand.REPORT_HEARTBEAT) {
            BReportJobMsg reportJobMsg = (BReportJobMsg) message.getBody();
            messageListener.reportJob(reportJobMsg.getNodeName(), reportJobMsg);
        }

        BInternalLogger.debug(BProtocolHandler.class, "It have processed a message [" + message + "]");
    }

    private void writeMsgBindResp() throws Exception {
        BMessage bindResp = new BMessage(BMessageCommand.BIND_RESP, 0, null);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bindResp.combineMessage());
        achannel.write(byteBuffer, this);
    }

    void writeMessage(BMessage message) throws Exception {
        ByteBuffer byteBuffer = ByteBuffer.wrap(message.combineMessage());
        achannel.write(byteBuffer, this);
    }

    void close() {
        achannel.close();
    }

}
