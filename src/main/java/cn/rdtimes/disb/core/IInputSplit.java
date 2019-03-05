package cn.rdtimes.disb.core;

import java.io.Serializable;

/**
 * 获取分片信息接口
 * 这是在主服务器端运行的
 * Created by BZ on 2019/2/12.
 */
public interface IInputSplit extends Serializable {

    /**
     * 根据节点数量进行分片,分片的数据不能超过nodes的数目
     * @param nodes 计算节点数量
     * @return
     * @throws Exception
     */
    BSplit[] getSplit(int nodes) throws Exception;

}
