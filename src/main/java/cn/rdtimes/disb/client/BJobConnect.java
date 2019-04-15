package cn.rdtimes.disb.client;

import cn.rdtimes.disb.core.BInternalLogger;
import cn.rdtimes.disb.protocol.*;
import cn.rdtimes.util.BStreamUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * 连接管理
 * 功能:
 * 1.连接主服务器,重复3次连续不上抛出异常
 * 2.循环读消息并通知处理消息
 * 3.写消息
 * Created by BZ.
 */
final class BJobConnect {
    //失败时最小连接间隔
    private final static int CONNECT_INTERVAL = 5000;
    //重复连接次数
    private final static int REPEAT_COUNT = 3;

    private Socket clientSocket;
    private InputStream inputStream;
    private OutputStream outputStream;

    //是否连接成功
    private volatile boolean isConnected;
    //线程停止标志
    private volatile boolean stopFlag;
    //线程锁
    private final Object lock = new Object();
    //指令监听处理器
    private final BJobLaunchImp.JobMessageListener commandListener;

    BJobConnect(BJobLaunchImp.JobMessageListener commandListener) {
        this.commandListener = commandListener;
    }

    private void startReceiveThread() {
        ReceiveMsgThread receiveMsgThread = new ReceiveMsgThread();
        receiveMsgThread.start();
    }

    boolean connect(String ip, int port) {
        int failureCount = 0;
        while (true) {
            try {
                connectToMaster(ip, port);
                BInternalLogger.info(BJobConnect.class,
                        "It have connected master [" + ip + ":" + port + "]");

                startReceiveThread();

                return true;
            } catch (UnknownHostException ue) { //hostname错误
                throw new IllegalArgumentException(ue);
            } catch (IOException e) {
                BInternalLogger.error(BJobConnect.class, "connect master error:", e);
                isConnected = false;
                ++failureCount;
            }

            if (failureCount >= REPEAT_COUNT) { //重复n次连接
                return false;
            }

            //运行到这里说明没有连接成功,休息一段时间继续尝试连接
            synchronized (lock) {
                //计算休息间隔
                int interval = CONNECT_INTERVAL;
                BInternalLogger.debug(BJobConnect.class,
                        "It is trying to connect master [" + ip + ":" + port + "(" + failureCount +
                                ")] after " + interval + " millisecond");
                try {
                    lock.wait(interval);
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        }
    }

    private void connectToMaster(String ip, int port) throws IOException {
        clientSocket = new Socket(ip, port);
        inputStream = clientSocket.getInputStream();
        outputStream = clientSocket.getOutputStream();
        isConnected = true;
    }

    //写消息
    void writeMessage(BMessage message) throws Exception {
        //避免在未连接时消息泛滥
        if (!isConnected) {
            throw new Exception("not connection");
        }

        byte[] data;
        try {
            data = message.combineMessage();
            outputStream.write(data);

            BInternalLogger.debug(BJobConnect.class, "It have sent a message [" + message + "]");
        } catch (Exception e) {
            BInternalLogger.error(BJobConnect.class, "write message error:", e);
            throw e;
        }
    }

    void close() {
        if (!isConnected && stopFlag) {
            return;
        }

        isConnected = false;
        stopFlag = true;
        //通知线程
        synchronized (lock) {
            lock.notify();
        }

        //关闭socket
        BStreamUtil.close(inputStream);
        BStreamUtil.close(outputStream);
        BStreamUtil.close(clientSocket);
    }

    //消息接收线程
    private class ReceiveMsgThread extends Thread {
        ReceiveMsgThread() {
            super("Client-ReceiveMsgThread");
        }

        public void run() {
            if (!isConnected) return;

            BInternalLogger.info(BJobConnect.class, getName() + " have been started.");

            //循环读取,这里是阻塞的
            while (!stopFlag) {
                try {
                    BMessage message = getMsgHead();
                    message = getMsgBody(message);
                    if (parseMessage(message)) { //返回true表示要求close
                        stopFlag = true;
                        break;
                    }
                } catch (IOException e) {
                    BInternalLogger.error(BJobConnect.class, "io error:", e);
                    stopFlag = true;
                    commandListener.close(null);
                    break;
                } catch (Exception e) {
                    BInternalLogger.error(BJobConnect.class, "parse message error:", e);
                }
            } //end while

            BInternalLogger.info(BJobConnect.class, getName() + " is over.");
        }

        //获取头部
        BMessage getMsgHead() throws Exception {
            byte[] head = new byte[BMessage.HEAD_SIZE];
            if (BStreamUtil.read(inputStream, head, 0, head.length) == -1) {
                throw new IOException("stream is eof");
            }

            return BMessage.parseMessageHead(head);
        }

        //获取消息体
        BMessage getMsgBody(BMessage message) throws Exception {
            if (message == null) return null;

            byte[] body = new byte[message.getLength()];
            if (BStreamUtil.read(inputStream, body, 0, body.length) == -1) {
                throw new IOException("stream is eof");
            }

            return BMessage.parseMessageBody(body, message);
        }

        //分析消息
        boolean parseMessage(BMessage message) throws Exception {
            if (message == null) return false;

            BInternalLogger.debug(BJobConnect.class, "It have received a message [" + message + "]");

            if (message.getCommand() == BMessageCommand.CLIENT_REPORT_HEARTBEAT) {
                commandListener.heartbeat();
            } else if (message.getCommand() == BMessageCommand.CLIENT_START_JOB_RESP) {
                commandListener.startJobResp((BClientStartJobRespMsg) message.getBody());
            } else if (message.getCommand() == BMessageCommand.CLIENT_CLOSE) {
                commandListener.close((BClientCloseJobMsg) message.getBody());
                return true;
            }

            return false;
        }

    }

}