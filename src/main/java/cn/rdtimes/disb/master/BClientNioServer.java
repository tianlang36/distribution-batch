package cn.rdtimes.disb.master;

import cn.rdtimes.disb.core.BInternalLogger;
import cn.rdtimes.disb.core.BJobState;
import cn.rdtimes.disb.protocol.*;
import cn.rdtimes.nio.lf.BAcceptor;
import cn.rdtimes.nio.lf.BDemultiplexor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * nio客户端服务器
 * 功能:
 * 1.读写消息
 * 2.通知处理消息
 * 3.管理通道
 * Created by BZ.
 */
final class BClientNioServer {
    private final BClientProtocolHandlerFactory handlerFactory;
    //节点名称对应的通道,key=jobId
    private final ConcurrentHashMap<String, BClientProtocolHandler> channelMap;
    private BDemultiplexor demultiplexor;

    BClientNioServer() {
        channelMap = new ConcurrentHashMap<String, BClientProtocolHandler>(3);
        handlerFactory = new BClientProtocolHandlerFactory(new MessageListener());

        try {
            demultiplexor = new BDemultiplexor();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void startAcceptors() throws Exception {
        InetSocketAddress address = new InetSocketAddress(BMasterConf.getInstance().getClientPort());
        BAcceptor acceptor = new BAcceptor(demultiplexor, address, handlerFactory);
        acceptor.start(1);

        BInternalLogger.info(BClientNioServer.class, "ClientNioServer accept on " + address);
    }

    private void printStatistics() {
        BInternalLogger.debug(BClientNioServer.class, handlerFactory.toString());
    }

    void start() {
        demultiplexor.start(BMasterConf.getInstance().getClientNioThreadCount());
        try {
            startAcceptors();

            //心跳线程
            HeartbeatThread heartbeatThread = new HeartbeatThread();
            heartbeatThread.start();
        } catch (Exception e) {
            try {
                demultiplexor.shutdown();
            } catch (Exception e1) {
                //ignore
            }

            throw new RuntimeException("client start error", e);
        }
    }

    void close() {
        stopFlag = true;
        synchronized (lock) {
            lock.notify();
        }

        if (demultiplexor != null) {
            try {
                demultiplexor.shutdown();
            } catch (Exception e) {
                BInternalLogger.error(BClientNioServer.class, "client shutdown error:", e);
            }
        }

        printStatistics();
    }

    void writeMessage(String jobId, BMessage message) throws Exception {
        BClientProtocolHandler handler = channelMap.get(jobId);
        if (handler != null) {
            handler.writeMessage(message);
        }
    }

    void removeAndCloseChannel(String jobId) {
        channelMap.remove(jobId); //让客户端主动关闭
    }

    private class MessageListener implements IClientMessageListener {

        //启动任务
        public void startJob(BClientStartJobMsg startJobMsg, BClientProtocolHandler attachment) {
            BClientStartJobRespMsg jobRespMsg;
            jobRespMsg = BMasterService.getJobManager().startJob(startJobMsg.getJobClassName(),
                    startJobMsg.getInputSplit());
            if (jobRespMsg.getJobState() != BJobState.EXCEPTION) {
                //注册任务
                channelMap.put(jobRespMsg.getJobId(), attachment);
            }

            try {
                //发送响应或关闭消息
                BMessage<BClientStartJobRespMsg> message = new BMessage<BClientStartJobRespMsg>(
                        BMessageCommand.CLIENT_START_JOB_RESP, 0, jobRespMsg);
                attachment.writeMessage(message);

                BInternalLogger.debug(BClientNioServer.class, "It have sent a message [" + message + "]");

                //发送关闭消息如果发现异常了
                if (jobRespMsg.getJobState() == BJobState.EXCEPTION) {
                    Thread.sleep(500); //延迟一下发送

                    BClientCloseJobMsg jobMsg = new BClientCloseJobMsg();
                    jobMsg.setJobId(jobRespMsg.getJobId());
                    jobMsg.setFailureCount(1);
                    jobMsg.setTotalCount(1);
                    jobMsg.setCause(jobRespMsg.getCause().getMessage());

                    BMessage<BClientCloseJobMsg> msg = new BMessage<BClientCloseJobMsg>(
                            BMessageCommand.CLIENT_CLOSE, 0, jobMsg);
                    attachment.writeMessage(msg);

                    BInternalLogger.debug(BClientNioServer.class, "It have sent a message [" + msg + "]");
                }
            } catch (Exception e) {
                BInternalLogger.error(BClientNioServer.class, "startJob error:", e);
            }
        }

        public void stopJob(String jobId, BClientProtocolHandler attachment) {
            try {
                BMasterService.getJobManager().stopJob(jobId);
            } catch (Exception e) {
                //异常发送关闭消息
                BClientCloseJobMsg jobMsg = new BClientCloseJobMsg();
                jobMsg.setJobId(jobId);
                jobMsg.setFailureCount(1);
                jobMsg.setTotalCount(1);
                jobMsg.setCause(e.getMessage());

                BMessage<BClientCloseJobMsg> message = new BMessage<BClientCloseJobMsg>(
                        BMessageCommand.CLIENT_CLOSE, 0, jobMsg);
                try {
                    attachment.writeMessage(message);

                    BInternalLogger.debug(BClientNioServer.class, "It have sent a message [" +
                            message + "]");
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }

        public void close(BClientProtocolHandler attachment) {
            Set<Map.Entry<String, BClientProtocolHandler>> entries = channelMap.entrySet();
            for (Map.Entry<String, BClientProtocolHandler> entry : entries) {
                if (entry.getValue().equals(attachment)) {
                    channelMap.remove(entry.getKey());
                    return;
                }
            }
        }

    }

    private volatile boolean stopFlag;
    private final Object lock = new Object();

    //向客户端定时发送心跳消息
    private class HeartbeatThread extends Thread {
        HeartbeatThread() {
            super("Master-ClientHeartbeatThread");
        }

        public void run() {
            BInternalLogger.info(HeartbeatThread.class, getName() + " have been started.");

            while (!stopFlag) {
                Set<Map.Entry<String, BClientProtocolHandler>> entries = channelMap.entrySet();
                for (Map.Entry<String, BClientProtocolHandler> entry : entries) {
                    BClientHeartbeatMsg clientHeartbeatMsg = new BClientHeartbeatMsg();
                    clientHeartbeatMsg.setJobId(entry.getKey());
                    BMessage<BClientHeartbeatMsg> message = new BMessage<BClientHeartbeatMsg>
                            (BMessageCommand.CLIENT_REPORT_HEARTBEAT, 0, clientHeartbeatMsg);
                    try {
                        entry.getValue().writeMessage(message);
                    } catch (Exception e) {
                        BInternalLogger.error(HeartbeatThread.class, "send message error:", e);
                    }
                }

                synchronized (lock) {
                    try {
                        lock.wait(BMasterConf.getInstance().getClientHeartbeatInterval());
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
            } //end while

            BInternalLogger.info(HeartbeatThread.class, getName() + " is over.");
        }
    }

}
