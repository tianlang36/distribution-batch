package cn.rdtimes.disb.master.exception;

/**
 * 没有找到足够的节点服务器异常
 * Created by BZ.
 */
public class BNoneEnoughNodeException extends Exception {

    public BNoneEnoughNodeException() {
        super();
    }

    public BNoneEnoughNodeException(String message, Throwable cause) {
        super(message, cause);
    }

    public BNoneEnoughNodeException(Throwable cause) {
        super(cause);
    }

    public BNoneEnoughNodeException(String message) {
        super(message);
    }

}
