package notification;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.exceptions.NotificationException;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @ClassName KafkaNotification
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/22 15:57
 * @Version 1.0
 **/
public class KafkaNotification extends AbstractNotification {
    public static final Logger logger = LoggerFactory.getLogger(KafkaNotification.class);

    public static final String HOOK_TOPIC = "HIVE_HOOK";
    protected static final String CONSUMER_GROUP_ID_PROPERTY = "hive-hook";
    private final Properties properties;

    private KafkaProducer<String, String> producer;

    /**
     * Construct a KafkaNotification.
     *
     * @param applicationProperties
     * @throws Exception
     */
    @Inject
    public KafkaNotification(Configuration applicationProperties) throws Exception {
        super(applicationProperties);

        properties = ConfigurationConverter.getProperties(applicationProperties);

        //Override default configs
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        boolean oldApiCommitEnableFlag = applicationProperties.getBoolean("auto.commit.enable", false);

        //set old autocommit value if new autoCommit property is not set.
        properties.put("enable.auto.commit", applicationProperties.getBoolean("enable.auto.commit", oldApiCommitEnableFlag));
        properties.put("session.timeout.ms", applicationProperties.getString("session.timeout.ms", "30000"));
    }

    @Override
    protected void sendInternal(List<String> messages) throws NotificationException {
        if (producer == null) {
            createProducer();
        }
        sendInternalToProducer(producer, messages);
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    private synchronized void createProducer() {
        if (producer == null) {
            producer = new KafkaProducer<>(properties);
        }
    }


    private class MessageContext {
        private final Future<RecordMetadata> future;
        private final String message;

        public MessageContext(Future<RecordMetadata> future, String message) {
            this.future = future;
            this.message = message;
        }

        public Future<RecordMetadata> getFuture() {
            return future;
        }

        public String getMessage() {
            return message;
        }
    }

    /**
     * 发送消息
     *
     * @param p        Producer
     * @param messages
     * @throws NotificationException
     */
    void sendInternalToProducer(Producer<String, String> p, List<String> messages) throws NotificationException {
        List<MessageContext> messageContexts = new ArrayList<>();

        for (String message : messages) {
            if (StringUtils.isNotBlank(message)) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(HOOK_TOPIC, message);

                if (logger.isDebugEnabled()) {
                    logger.debug("Sending message for topic {}: {}", HOOK_TOPIC, message);
                }

                Future<RecordMetadata> future = p.send(record);

                messageContexts.add(new MessageContext(future, message));
            }
        }
        List<String> failedMessages = new ArrayList<>();
        Exception lastFailureException = null;

        for (MessageContext context : messageContexts) {
            try {
                RecordMetadata response = context.getFuture().get();

                if (logger.isDebugEnabled()) {
                    logger.debug("Sent message for topic - {}, partition - {}, offset - {}",
                            response.topic(), response.partition(), response.offset());
                }
            } catch (Exception e) {
                lastFailureException = e;

                failedMessages.add(context.getMessage());
            }
        }
        if (lastFailureException != null) {
            throw new NotificationException(lastFailureException, failedMessages);
        }
    }

}
