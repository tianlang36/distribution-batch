package cn.rdtimes.disb.core;


/**
 * 批处理任务,这个是具体业务逻辑要实现的接口
 * 这个是在节点服务器端运行的
 * <p>
 * Created by BZ on 2019/2/12.
 */
public interface IJob {

    /**
     * 返回任务id
     * @return
     */
    String getId();

    /**
     * 设置任务id
     * @param id
     */
    void setId(String id);

    /**
     * 设置组件对应的分片信息
     * @param split
     */
    void setSplit(BSplit split);

    /**
     * 返回组件对应的分片信息
     * @return
     */
    BSplit getSplit();

    /**
     * 启动组件,开始执行任务
     */
    void start() throws Exception;

    /**
     * 停止组件
     */
    void stop();

    /**
     * 等待组件完成,除非调用stop方法
     */
    void waitCompleted();

}
