package cn.rdtimes.disb.master;

import cn.rdtimes.disb.core.BInternalLogger;
import cn.rdtimes.disb.master.exception.BNoneEnoughNodeException;
import cn.rdtimes.disb.protocol.BMessage;
import cn.rdtimes.disb.protocol.BReportJobMsg;
import cn.rdtimes.nio.lf.BAcceptor;
import cn.rdtimes.nio.lf.BDemultiplexor;
import cn.rdtimes.util.BStringUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * nio服务器
 * 功能:
 * 1.读写消息
 * 2.通知处理消息
 * 3.管理通道
 * Created by BZ.
 */
final class BNioServer {
    private final BProtocolHandlerFactory handlerFactory;
    //节点名称对应的通道,key=节点名称
    private final ConcurrentHashMap<String, BProtocolHandler> channelMap;
    private BDemultiplexor demultiplexor;

    BNioServer() {
        channelMap = new ConcurrentHashMap<String, BProtocolHandler>(3);
        handlerFactory = new BProtocolHandlerFactory(new MessageListener());
        try {
            demultiplexor = new BDemultiplexor();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void startAcceptors() throws Exception {
        InetSocketAddress address = new InetSocketAddress(BMasterConf.getInstance().getPort());
        BAcceptor acceptor = new BAcceptor(demultiplexor, address, handlerFactory);
        acceptor.start(1);

        BInternalLogger.info(BNioServer.class, "NioServer accept on " + address);
    }

    private void printStatistics() {
        BInternalLogger.debug(BNioServer.class, handlerFactory.toString());
    }

    void start() {
        demultiplexor.start(BMasterConf.getInstance().getNioThreadCount());
        try {
            startAcceptors();
        } catch (Exception e) {
            try {
                demultiplexor.shutdown();
            } catch (Exception e1) {
                //ignore
            }

            throw new RuntimeException("start error", e);
        }
    }

    void close() {
        if (demultiplexor != null) {
            try {
                demultiplexor.shutdown();
            } catch (Exception e) {
                BInternalLogger.error(BNioServer.class, "shutdown error:", e);
            }
        }

        printStatistics();
    }

    int getActiveNodeCount() {
        return channelMap.size();
    }

    /**
     * 写启动任务消息
     *
     * @param messages 消息数量(应该不大于通道数量)
     * @return 发给节点的名称列表
     * @throws BNoneEnoughNodeException 没有足够数量的节点抛出此异常
     */
    String[] writeStartJobMessage(BMessage[] messages) throws Exception {
        if (messages.length > channelMap.size()) {
            throw new BNoneEnoughNodeException("not enough node host");
        }

        List<String> keyList = new ArrayList<String>(channelMap.keySet());
        String[] nodeNames = new String[messages.length];

        int i = 0;
        String nodeName;
        for (BMessage msg : messages) {
            nodeName = keyList.get(i);
            writeMessage(channelMap.get(nodeName), msg);
            nodeNames[i] = nodeName;
            ++i;
        }

        return nodeNames;
    }

    /**
     * 写停止任务消息到指定的节点上
     *
     * @param message 消息
     * @param hostNames 节点名称数组
     * @return 全部写成功返回true, 部分写成功返回false
     */
    boolean writeStopJobMessage(BMessage message, String[] hostNames) {
        boolean isSuccess = true;
        BProtocolHandler cc;
        for (String nodeName : hostNames) {
            cc = channelMap.get(nodeName);
            if (cc != null) {
                try {
                    writeMessage(cc, message);
                } catch (Exception e) {
                    isSuccess = false;
                }
            } else {
                isSuccess = false;
            }
        }

        return isSuccess;
    }

    private void writeMessage(BProtocolHandler handler, BMessage message) throws Exception {
        handler.writeMessage(message);
    }

    private class MessageListener implements IMessageListener {

        public void reportJob(String nodeHost, BReportJobMsg reportJobMsg) {
            //汇报给任务管理器
            BMasterService.getJobManager().jobReportNotify(nodeHost, reportJobMsg);
        }

        public void bind(String nodeHost, BProtocolHandler attachment) {
            //防止节点名称重复,后者将被强制关闭
            if (!channelMap.containsKey(nodeHost)) {
                channelMap.put(nodeHost, attachment);
            } else {
                attachment.close();
            }

            BInternalLogger.debug(MessageListener.class, "It is " + channelMap.size() +
                    " channels connection");
        }

        public void unbind(String nodeHost, BProtocolHandler attachment) {
            if (BStringUtil.isEmpty(nodeHost)) { //节点服务器非正常关闭
                Set<Map.Entry<String, BProtocolHandler>> entrySet = channelMap.entrySet();
                for (Map.Entry<String, BProtocolHandler> entry : entrySet) {
                    if (entry.getValue().equals(attachment)) {
                        channelMap.remove(entry.getKey());
                        return;
                    }
                }
            } else {
                channelMap.remove(nodeHost);
            }

            BInternalLogger.debug(MessageListener.class, "It is " + channelMap.size() +
                    " channels connection");
        }

    }

}
