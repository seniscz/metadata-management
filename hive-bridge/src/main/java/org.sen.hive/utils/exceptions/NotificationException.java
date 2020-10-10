package org.sen.hive.utils.exceptions;

import java.util.List;

/**
 * @ClassName NotificationException
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/22 15:35
 * @Version 1.0
 **/
public class NotificationException extends Exception{
    private List<String> failedMessages;

    public NotificationException(Exception e) {
        super(e);
    }

    public NotificationException(Exception e, List<String> failedMessages) {
        super(e);
        this.failedMessages = failedMessages;
    }

    public List<String> getFailedMessages() {
        return failedMessages;
    }
}
