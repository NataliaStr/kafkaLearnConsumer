package com.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.LibraryEventsConsumer;
import com.kafka.entity.Book;
import com.kafka.entity.LibraryEvent;
import com.kafka.entity.LibraryEventType;
import com.kafka.jpa.LibraryEventsRepository;
import com.kafka.jpa.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events", "library-events.RETRY", "library-events.DLT"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "retryListener.startup=false"})
class LibraryEventsConsumerIntegrationTest {

    //NUMBER_OF_INVOCATIONS is 3 because we have set backOff max attempt (retry) to 2 in the ConsumerConfig (one try + 2 retries = 3)
    public static final int NR_OF_INVOCATIONS = 3;

    public static final int NR_OF_INVOCATIONS_EX_NON_RETRYABLE= 1;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    @BeforeEach
    void setUp() {
        libraryEventsRepository.deleteAll();
        MessageListenerContainer container = endpointRegistry.getListenerContainers().stream().filter(messageListenerContainer ->
                        Objects.requireNonNull(messageListenerContainer.getGroupId()).equals("library-events-listener-group"))
                .toList().get(0);
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    }


    // Write a test to verify the consumer behavior when a new library event is produced
    @Test
    void publishNewLibraryEvent() throws Exception {
        //given
        String newLibraryEventJson = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "NEW",
                    "book": {
                        "bookId": 456,
                        "bookName": "Kafka Using Spring Boot",
                        "bookAuthor": "Dilip"
                    }
                }
                """;
        kafkaTemplate.sendDefault(newLibraryEventJson).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> all = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert all.size() == 1;
        all.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(LibraryEventType.NEW, libraryEvent.getLibraryEventType());
            assertEquals(456, libraryEvent.getBook().getBookId());
        });

    }

    // Write a test to verify the consumer behavior when an update library event is produced
    @Test
    void publishUpdateLibraryEvent() throws Exception {
        //given -- save initial library event
        String libraryEventJson = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "NEW",
                    "book": {
                        "bookId": 456,
                        "bookName": "Kafka Using Spring Boot",
                        "bookAuthor": "Dilip"
                    }
                }
                """;
        LibraryEvent libraryEvent = objectMapper.readValue(libraryEventJson, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        // publish update library event
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        Book book = Book.builder().bookId(456).bookName("Kafka Using Spring Boot 2.x").bookAuthor("Dilip").build();
        libraryEvent.setBook(book);
        String updatedLibraryEventJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedLibraryEventJson).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertNotNull(persistedLibraryEvent);
        assertEquals("Kafka Using Spring Boot 2.x", persistedLibraryEvent.getBook().getBookName());
        assertEquals(LibraryEventType.UPDATE,persistedLibraryEvent.getLibraryEventType());
    }


    @Test
    void publishUpdateLibraryEventWithoutEventId() throws Exception {
        //given -- save initial library event
        String libraryEventJson = """
                {
                    "libraryEventId": null,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 456,
                        "bookName": "Kafka Using Spring Boot",
                        "bookAuthor": "Dilip"
                    }
                }
                """;

        kafkaTemplate.sendDefault(libraryEventJson).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);

        //this suppose to be quicker than in other tests
        doAnswer(invocation -> {
            invocation.callRealMethod();
            latch.countDown();
            return null;
        }).when(libraryEventsConsumerSpy).onMessage(any(ConsumerRecord.class));
        kafkaTemplate.sendDefault(libraryEventJson).get();
        latch.await(5, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(NR_OF_INVOCATIONS_EX_NON_RETRYABLE)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(NR_OF_INVOCATIONS_EX_NON_RETRYABLE)).processLibraryEvent(isA(ConsumerRecord.class));
        // 10 is the default retry count for the Kafka consumer in case of an exception during processing the consumer record
    }


    @Test
    void publishUpdateLibraryEventOnNonExistingEvent() throws Exception {
        //given -- save initial library event
        String libraryEventJson = """
                {
                    "libraryEventId": 2,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 456,
                        "bookName": "Kafka Using Spring Boot",
                        "bookAuthor": "Dilip"
                    }
                }
                """;

        kafkaTemplate.sendDefault(libraryEventJson).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(NR_OF_INVOCATIONS_EX_NON_RETRYABLE)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(NR_OF_INVOCATIONS_EX_NON_RETRYABLE)).processLibraryEvent(isA(ConsumerRecord.class));
        // 10 is the default retry count for the Kafka consumer in case of an exception during processing the consumer record
    }


    @Test
    void publishUpdateLibraryEventWithEventId999() throws Exception {
        //given -- save initial library event
        String libraryEventJson = """
                {
                    "libraryEventId": 999,
                    "libraryEventType": "UPDATE",
                    "book": {
                        "bookId": 456,
                        "bookName": "Kafka Using Spring Boot",
                        "bookAuthor": "Dilip"
                    }
                }
                """;

        kafkaTemplate.sendDefault(libraryEventJson).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy, times(NR_OF_INVOCATIONS)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(NR_OF_INVOCATIONS)).processLibraryEvent(isA(ConsumerRecord.class));
        // 10 is the default retry count for the Kafka consumer in case of an exception during processing the consumer record



        //test further to check recovery topic
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        Consumer<Integer, String> consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer())
                .createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);

        System.out.println("consumerRecord is : " + consumerRecord.value());
        assertEquals(libraryEventJson, consumerRecord.value());
    }

}
