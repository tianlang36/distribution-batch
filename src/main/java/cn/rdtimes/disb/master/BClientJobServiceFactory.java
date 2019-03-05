package cn.rdtimes.disb.master;

/**
 * 为客户端使用的工厂,可以获取IJobManager接口
 * 将来主服务器独立部署时,此类将被抛弃
 *
 * Created by BZ on 2019/2/19.
 */
public final class BClientJobServiceFactory {

    public static IJobManager getJobManager() {
        return BMasterService.getJobManager();
    }

}
