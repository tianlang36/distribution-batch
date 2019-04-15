package cn.rdtimes.disb.master;

import cn.rdtimes.disb.protocol.*;
import cn.rdtimes.nio.lf.BAbstractAsynchHandlerFactory;
import cn.rdtimes.nio.lf.BAsynchChannel;
import cn.rdtimes.nio.lf.BFrameAsynchHandler;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * 抽象协议处理
 * Created by BZ.
 */
abstract class BAbstractProtocolHandler extends BFrameAsynchHandler {

    BAbstractProtocolHandler(BAbstractAsynchHandlerFactory factory) {
        super(factory);
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

    void writeMessage(BMessage message) throws Exception {
        ByteBuffer byteBuffer = ByteBuffer.wrap(message.combineMessage());
        achannel.write(byteBuffer, this);
    }

    void close() {
        achannel.close();
    }

}
