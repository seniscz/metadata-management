package org.sen.hive.utils.exceptions;

/**
 * @ClassName SQLParseException
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/22 15:36
 * @Version 1.0
 **/
public class SQLParseException extends RuntimeException{
    public SQLParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public SQLParseException(String message) {
        super(message);
    }

    public SQLParseException(Throwable cause) {
        super(cause);
    }

}
