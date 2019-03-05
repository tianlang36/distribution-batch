package cn.rdtimes.disb.master;

import cn.rdtimes.nio.lf.BAbstractAsynchHandlerFactory;
import cn.rdtimes.nio.lf.IAsynchChannelHandler;

/**
 * 协议处理工厂
 * Created by BZ on 2019/2/19.
 */
class BProtocolHandlerFactory extends BAbstractAsynchHandlerFactory {
    private final IMessageListener messageListener;

    BProtocolHandlerFactory(IMessageListener messageListener) {
        this.messageListener = messageListener;
    }

    protected IAsynchChannelHandler createAsynchChannelHandler() {
        try {
            return new BProtocolHandler(this, messageListener);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

}
