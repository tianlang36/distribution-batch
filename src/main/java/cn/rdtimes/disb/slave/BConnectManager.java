package cn.rdtimes.disb.slave;

import cn.rdtimes.disb.core.BInternalLogger;
import cn.rdtimes.disb.protocol.*;
import cn.rdtimes.util.BStreamUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 连接管理
 * 功能:
 * 1.连接主服务器,直到连接上或关闭为止
 * 2.循环读消息并通知处理消息
 * 3.写消息
 * Created by BZ.
 */
final class BConnectManager {
    //失败时最小连接间隔
    private final static int CONNECT_INTERVAL = 5000;
    //连接断开时,最多能写入队列中的消息数量
    private final static int MAX_MESSAGE_COUNT = 20;

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
    private final ICommandListener commandListener;
    //写消息队列
    private final ConcurrentLinkedQueue<BMessage<?>> writeQueue;

    BConnectManager(ICommandListener commandListener) {
        this.commandListener = commandListener;
        writeQueue = new ConcurrentLinkedQueue<BMessage<?>>();
    }

    void start() {
        ReceiveMsgThread receiveMsgThread = new ReceiveMsgThread();
        receiveMsgThread.start();

        SendMsgThread sendMsgThread = new SendMsgThread();
        sendMsgThread.start();
    }

    //每隔一段时间不断的进行连接尝试
    private void connect() {
        String ip = BNodeConf.getInstance().getMasterIp();
        int port = BNodeConf.getInstance().getMasterPort();

        int failureCount = 0;
        while (!stopFlag) { //查看停止标志
            try {
                connectToMaster(ip, port);
                failureCount = 0;
                BInternalLogger.info(BConnectManager.class,
                        "It have connected master [" + ip + ":" + port + "]");

                break;
            } catch (UnknownHostException ue) { //hostname错误
                throw new IllegalArgumentException(ue);
            } catch (IOException e) {
                BInternalLogger.error(BConnectManager.class, "connect master error:", e);
                isConnected = false;
                ++failureCount;
            }

            //运行到这里说明没有连接成功,休息一段时间继续尝试连接
            synchronized (lock) {
                //计算休息间隔
                int interval = calcInterval(failureCount);
                BInternalLogger.debug(BConnectManager.class,
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

        sendBind();

        //通知写线程可以发送了
        synchronized (writeQueue) {
            writeQueue.notify();
        }
    }

    private int calcInterval(int count) {
        int tryMaxWaitConnectionTime = BNodeConf.getInstance().getTryMaxWaitConnection();
        int interval = CONNECT_INTERVAL * count;
        return (interval > tryMaxWaitConnectionTime ? 60000 : CONNECT_INTERVAL);
    }

    private void sendBind() throws IOException {
        BBindMsg bindMsg = new BBindMsg();
        bindMsg.setNodeName(BNodeConf.getInstance().getNodeName());
        BMessage<BBindMsg> message = new BMessage<BBindMsg>(BMessageCommand.BIND, 0, bindMsg);

        outputStream.write(message.combineMessage());
    }

    private void sendUnbind() {
        BUnbindMsg unbindMsg = new BUnbindMsg();
        unbindMsg.setNodeName(BNodeConf.getInstance().getNodeName());
        BMessage<BUnbindMsg> message = new BMessage<BUnbindMsg>(BMessageCommand.UNBIND, 0, unbindMsg);
        try {
            outputStream.write(message.combineMessage());
        } catch (Exception e) {
            BInternalLogger.error(BConnectManager.class, "send unbind error:", e);
        }
    }

    //写消息
    void writeMessage(BMessage message) {
        //避免在未连接时消息泛滥
        if (!isConnected && writeQueue.size() > MAX_MESSAGE_COUNT) {
            return;
        }

        writeQueue.offer(message);
        synchronized (writeQueue) {
            writeQueue.notify();
        }
    }

    void close() {
        //如果正常连接发送unbind消息
        if (isConnected) {
            sendUnbind();
        }

        isConnected = false;
        stopFlag = true;
        //通知线程
        synchronized (lock) {
            lock.notify();
        }
        synchronized (writeQueue) {
            writeQueue.notify();
        }
        //关闭socket
        BStreamUtil.close(inputStream);
        BStreamUtil.close(outputStream);
        BStreamUtil.close(clientSocket);
    }

    //消息接收线程
    private class ReceiveMsgThread extends Thread {
        ReceiveMsgThread() {
            super("Node-ReceiveMsgThread");
        }

        public void run() {
            BInternalLogger.info(BConnectManager.class, getName() + " have been started.");

            //开始连接操作,连接成功才能继续
            connect();

            //循环读取,这里是阻塞的
            while (!stopFlag) {
                try {
                    BMessage message = getMsgHead();
                    message = getMsgBody(message);
                    parseMessage(message);
                } catch (IOException e) {
                    isConnected = false;
                    if (!stopFlag) {
                        connect();
                    }
                } catch (Exception e) {
                    BInternalLogger.error(BConnectManager.class, "parse message error:", e);
                }
            } //end while

            BInternalLogger.info(BConnectManager.class, getName() + " is over.");
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
        void parseMessage(BMessage message) throws Exception {
            if (message == null) return;

            BInternalLogger.debug(BConnectManager.class,
                    "It have received a message [" + message + "]");

            if (message.getCommand() == BMessageCommand.START_JOB) { //启动任务通知
                commandListener.startJob((BStartJobMsg) message.getBody());
            } else if (message.getCommand() == BMessageCommand.STOP_JOB) { //停止任务通知
                commandListener.stopJob((BStopJobMsg) message.getBody());
            }
        }

    }

    //消息发送线程
    private class SendMsgThread extends Thread {
        SendMsgThread() {
            super("Node-SendMsgThread");
        }

        public void run() {
            BInternalLogger.info(BConnectManager.class, getName() + " have been started.");

            while (!stopFlag) {
                BMessage<?> message;
                while (isConnected && !stopFlag && (message = writeQueue.peek()) != null) {
                    byte[] data;
                    try {
                        data = message.combineMessage();
                    } catch (Exception e) {
                        writeQueue.poll();  //错误的消息下次不能再发送了
                        BInternalLogger.error(BConnectManager.class, "combine message error:", e);
                        continue;
                    }

                    //发送消息
                    try {
                        outputStream.write(data);

                        BInternalLogger.debug(BConnectManager.class,
                                "It have sent a message [" + message + "]");
                    } catch (IOException e) { //连接出现问题了,需要重新连接.这时读线程应该也会出现错误进行重新连接
                        break;
                    }

                    //成功输出后再删除消息
                    writeQueue.poll();
                }

                synchronized (writeQueue) {
                    try {
                        writeQueue.wait();
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
            } //end while

            BInternalLogger.info(BConnectManager.class, getName() + " is over.");
        }

    }

}