package cn.rdtimes.disb.slave;

import cn.rdtimes.disb.core.BInternalLogger;

/**
 * 节点服务器启动类
 * <p>
 * 启动节点服务器:
 * java cn.rdtimes.disb.slave.BNodeMain [confFileName]
 * <p>
 * Created by BZ on 2019/2/14.
 */
public final class BNodeMain {
    private final BJobScheduler jobScheduler;

    BNodeMain(String[] args) {
        BNodeConf.getInstance().readConf(args.length > 0 ? args[0] : null);

        jobScheduler = new BJobScheduler();
    }

    private void registryShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
    }

    private void start() {
        jobScheduler.start();
        registryShutdownHook();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            //ignore
        }

        BInternalLogger.info(BNodeMain.class, "Node service have been started.");
    }

    private void shutdown() {
        jobScheduler.shutdown();

        BInternalLogger.info(BNodeMain.class, "Node service have been shutdown.");
    }

    public static void main(String[] args) {
        //可以传递一个配置文件路径的参数 main /file/slave.properties,
        //如果不传递就使用默认路径下的配置信息
        BNodeMain slaveMain = new BNodeMain(args);
        slaveMain.start();
    }

}
