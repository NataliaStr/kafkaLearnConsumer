package com.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class ConsumerConfig {

    final KafkaTemplate<Object,Object> kafkaTemplate;

    @Value("${topics.retry:library-events.RETRY}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    public ConsumerConfig(KafkaTemplate<Object,Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // publish the error message to the recovery topic
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer() {

        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        log.error("Sending to Retry Topic");
                        return new TopicPartition(retryTopic, r.partition());
                    } else {
                        log.error("Sending to DLT");
                        return new TopicPartition(deadLetterTopic, r.partition());
                    }
                }
        );
    }

    @Bean
    public DefaultErrorHandler errorHandler() {
        FixedBackOff backOff = new FixedBackOff(1000L, 2);
        // Alternative to FixedBackOff for error handling
//        ExponentialBackOff expBackOff = new ExponentialBackOffWithMaxRetries(999999);
//        expBackOff.setInitialInterval(1000L); // 1 second interval between retries
//        expBackOff.setMultiplier(2.0); // 2x multiplier for each subsequent retry
//        expBackOff.setMaxInterval(10_000L); // max interval of 10 seconds

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer(),
                backOff);

        var exceptionsToIgnore = List.of(
                IllegalArgumentException.class
        );
        exceptionsToIgnore.forEach(errorHandler::addNotRetryableExceptions);
//       // Alternative to addNotRetryableExceptions is defining addRetryableExceptions
//        var exceptionsToRetry = List.of(
//                RecoverableDataAccessException.class,
//                JsonProcessingException.class
//        );
//        exceptionsToRetry.forEach(errorHandler::addRetryableExceptions);

        errorHandler.setRetryListeners((failedRecord, exception, deliveryAttempt) ->
                log.info("Failed record in Retry Listener, Exception: {}, deliveryAttempt: {}",
                        exception.getMessage(), deliveryAttempt));

        return errorHandler;
    }

    // TODO: Custom ConsumerRecordRecoverer
    ConsumerRecordRecoverer consumerRecordRecoverer = (recordToRecover, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, recordToRecover);
        if (exception.getCause() instanceof RecoverableDataAccessException) {
            log.info("Inside the recoverable logic");
            //TODO: Add any Recovery Code here.
            //failureService.saveFailedRecord((ConsumerRecord<Integer, String>) record, exception, RETRY);

        } else {
            log.info("Inside the non recoverable logic and skipping the record : {}", recordToRecover);

        }
    };

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }
}
