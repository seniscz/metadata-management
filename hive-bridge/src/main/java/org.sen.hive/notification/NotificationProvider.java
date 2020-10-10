package org.sen.hive.notification;

import org.apache.commons.configuration.Configuration;
import org.sen.hive.utils.ApplicationProperties;

/**
 * @ClassName NotificationProvider
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/22 15:51
 * @Version 1.0
 **/
public class NotificationProvider {
    private static KafkaNotification kafka;

    public static KafkaNotification get() {
        if (kafka == null) {
            try {
                Configuration applicationProperties = ApplicationProperties.get();
                kafka = new KafkaNotification(applicationProperties);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return kafka;
    }

}
