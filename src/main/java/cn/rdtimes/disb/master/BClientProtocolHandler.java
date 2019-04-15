package cn.rdtimes.disb.master;

import cn.rdtimes.disb.core.BInternalLogger;
import cn.rdtimes.disb.protocol.*;
import cn.rdtimes.nio.lf.BAsynchChannel;

import java.nio.ByteBuffer;

/**
 * 客户端协议处理
 * Created by BZ.
 */
class BClientProtocolHandler extends BAbstractProtocolHandler {
    private final IClientMessageListener messageListener;

    BClientProtocolHandler(BClientProtocolHandlerFactory f, IClientMessageListener messageListener) {
        super(f);
        this.messageListener = messageListener;
    }

    public void channelClosed(BAsynchChannel channel) throws Exception {
        messageListener.close(this);
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

        if (message.getCommand() == BMessageCommand.CLIENT_START_JOB) {
            messageListener.startJob(((BClientStartJobMsg) message.getBody()), this);
        } else if (message.getCommand() == BMessageCommand.CLIENT_STOP_JOB) {
            messageListener.stopJob(((BClientStopJobMsg) message.getBody()).getJobId(), this);
        }

        BInternalLogger.debug(BClientProtocolHandler.class, "It have processed a message [" + message + "]");
    }

}
