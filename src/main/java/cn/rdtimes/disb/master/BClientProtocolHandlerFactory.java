package cn.rdtimes.disb.master;

import cn.rdtimes.nio.lf.BAbstractAsynchHandlerFactory;
import cn.rdtimes.nio.lf.IAsynchChannelHandler;

/**
 * 客户端协议处理工厂
 * Created by BZ.
 */
class BClientProtocolHandlerFactory extends BAbstractAsynchHandlerFactory {
    private final IClientMessageListener messageListener;

    BClientProtocolHandlerFactory(IClientMessageListener messageListener) {
        this.messageListener = messageListener;
    }

    protected IAsynchChannelHandler createAsynchChannelHandler() {
        try {
            return new BClientProtocolHandler(this, messageListener);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

}
