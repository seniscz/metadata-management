package notification;

import utils.exceptions.NotificationException;

import java.util.List;

/**
 * @ClassName NotificationInterface
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/22 15:44
 * @Version 1.0
 **/
public interface NotificationInterface {

    /**
     * 发送指定的消息
     *
     * @param messages
     * @throws NotificationException if an error occurs while sending
     */
    void send(String... messages) throws NotificationException;

    /**
     * 发送指定的消息
     *
     * @param messages
     * @throws NotificationException if an error occurs while sending
     */
    void send(List<String> messages) throws NotificationException;

    /**
     * Shutdown any notification producers and consumers associated with this interface instance.
     */
    void close();
}
