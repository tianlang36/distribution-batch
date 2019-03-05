package cn.rdtimes.disb.master;


import cn.rdtimes.util.BStringUtil;

/**
 * 服务接口
 * <p>
 * 功能:
 * 1.启动主服务的入口(包括nio服务等)
 * 2.获取任务管理接口
 * <p>
 * <p>
 * 启动服务:
 * MasterService.start();
 * 停止服务:
 * MasterService.shutdown();
 * 调用顺序:
 * client->master->node
 * <p>
 * Created by BZ on 2019/2/19.
 */
public final class BMasterService {
    private static BJobManager jobManager;
    private static BNioServer nioServer;

    public BMasterService(String confFileName) {
        BMasterConf.getInstance().readConf(BStringUtil.isEmpty(confFileName) ? null : confFileName);

        jobManager = new BJobManager();
        nioServer = new BNioServer();
    }

    static BJobManager getJobManager() {
        return jobManager;
    }

    static BNioServer getNioServer() {
        return nioServer;
    }

    public void start() {
        jobManager.start();
        nioServer.start();
    }

    public void shutdown() {
        nioServer.close();
        jobManager.shutdown();
    }

}
