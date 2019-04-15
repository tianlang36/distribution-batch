package cn.rdtimes.disb.master;

import cn.rdtimes.disb.core.BInternalLogger;
import cn.rdtimes.disb.protocol.*;
import cn.rdtimes.nio.lf.BAsynchChannel;

import java.nio.ByteBuffer;

/**
 * 协议处理
 * Created by BZ.
 */
class BProtocolHandler extends BAbstractProtocolHandler {
    private final IMessageListener messageListener;
    private volatile boolean isNormalClose;

    BProtocolHandler(BProtocolHandlerFactory f, IMessageListener messageListener) {
        super(f);
        this.messageListener = messageListener;
    }

    public void channelClosed(BAsynchChannel channel) throws Exception {
        if (!isNormalClose) {
            messageListener.unbind(null, this);
            BInternalLogger.debug(BProtocolHandler.class, "node channel is not normal close");
        }
        super.channelClosed(channel);
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
            isNormalClose = true;
            messageListener.unbind(((BUnbindMsg) message.getBody()).getNodeName(), this);
        } else if (message.getCommand() == BMessageCommand.REPORT_HEARTBEAT) {
            BReportJobMsg reportJobMsg = (BReportJobMsg) message.getBody();
            messageListener.reportJob(reportJobMsg.getNodeName(), reportJobMsg);
        }

        BInternalLogger.debug(BProtocolHandler.class, "It have processed a message [" + message + "]");
    }

    private void writeMsgBindResp() throws Exception {
        BMessage bindResp = new BMessage<String>(BMessageCommand.BIND_RESP, 0, null);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bindResp.combineMessage());
        achannel.write(byteBuffer, this);
    }

}
