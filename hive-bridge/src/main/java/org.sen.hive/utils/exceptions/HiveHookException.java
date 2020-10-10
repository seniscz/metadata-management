package org.sen.hive.utils.exceptions;

/**
 * @ClassName HiveHookException
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/22 11:07
 * @Version 1.0
 **/
public class HiveHookException extends Exception {

    public HiveHookException() {

    }

    public HiveHookException(String message) {
        super(message);
    }

    public HiveHookException(String message, Throwable cause) {
        super(message, cause);
    }

    public HiveHookException(Throwable cause) {
        super(cause);
    }

    public HiveHookException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }


}
