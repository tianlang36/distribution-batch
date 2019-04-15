package cn.rdtimes.disb.protocol;

import java.io.Serializable;

/**
 * 退出消息体
 * Created by BZ.
 */
public class BUnbindMsg implements Serializable {
    private final static long serialVersionUID = -1;

    //节点名称
    private String nodeName;

    public BUnbindMsg() {}

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
