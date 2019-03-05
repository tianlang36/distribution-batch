package cn.rdtimes.disb.protocol;

import java.io.Serializable;

/**
 * 登录消息体
 * Created by BZ on 2019/2/13.
 */
public class BBindMsg implements Serializable {
    private final static long serialVersionUID = -1;

    //节点名称
    private String nodeName;

    public BBindMsg() {}

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String toString() {
        return "nodeName:" + nodeName;
    }

}
