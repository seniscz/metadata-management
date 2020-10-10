package notification;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.configuration.Configuration;
import utils.exceptions.HiveHookException;
import utils.exceptions.NotificationException;

import java.util.List;

/**
 * @ClassName AbstractNotification
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/22 15:52
 * @Version 1.0
 **/
public abstract class AbstractNotification implements NotificationInterface {
    /**
     * each char can encode upto 4 bytes in UTF-8
     */
    public static final int MAX_BYTES_PER_CHAR = 4;

    public AbstractNotification(Configuration applicationProperties) throws HiveHookException {
    }

    @VisibleForTesting
    protected AbstractNotification() {
    }

    @Override
    public void send(String... messages) throws NotificationException {

    }

    @Override
    public void send(List<String> messages) throws NotificationException {

    }

    /**
     * Send the given messages.
     *
     * @param messages
     * @throws NotificationException
     */
    protected abstract void sendInternal(List<String> messages) throws NotificationException;


}
