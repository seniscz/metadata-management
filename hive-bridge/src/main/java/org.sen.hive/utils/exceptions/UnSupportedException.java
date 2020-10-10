package org.sen.hive.utils.exceptions;

/**
 * @ClassName UnSupportedException
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/22 15:41
 * @Version 1.0
 **/
public class UnSupportedException extends RuntimeException {
    public UnSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnSupportedException(String message) {
        super(message);
    }

    public UnSupportedException(Throwable cause) {
        super(cause);
    }
}
