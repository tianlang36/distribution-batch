package cn.rdtimes.disb.master;

import cn.rdtimes.disb.core.BInternalLogger;

/**
 * 主服务器启动类(下个版本实现)
 * 主服务启动:
 * java cn.rdtimes.disb.master.MasterMain [confFileName]
 *
 * Created by BZ on 2019/2/20.
 */
final class BMasterMain {
    private final BMasterService masterService;

    BMasterMain(String[] args) {
        masterService = new BMasterService(args.length == 0 ? null : args[0]);
    }

    private void registryShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
    }

    void start() {
        masterService.start();
        registryShutdownHook();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            //ignore
        }

        BInternalLogger.info(BMasterMain.class, "Master service has been started.");
    }

    void shutdown() {
        masterService.shutdown();

        BInternalLogger.info(BMasterMain.class, "Master service has been shutdown.");
    }

    public static void main(String[] args) {
        BMasterMain masterMain = new BMasterMain(args);
        masterMain.start();
    }

}
