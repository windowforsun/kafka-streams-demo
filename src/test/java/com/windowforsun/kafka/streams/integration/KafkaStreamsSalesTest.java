package com.windowforsun.kafka.streams.integration;

import com.windowforsun.kafka.streams.KafkaConfig;
import com.windowforsun.kafka.streams.Util;
import com.windowforsun.kafka.streams.event.PaymentEvent;
import com.windowforsun.kafka.streams.event.SalesEvent;
import com.windowforsun.kafka.streams.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = KafkaConfig.class)
@EmbeddedKafka(controlledShutdown = true, topics = {"sales-topic", "db-store-topic", "es-store-topic"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
public class KafkaStreamsSalesTest {
    private final static String SALES_TOPIC = "sales-topic";
    @Autowired
    private KafkaTemplate testKafkaTemplate;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private TestRestTemplate testRestTemplate;
    @Autowired
    private KafkaDbStoreListener dbStoreListener;
    @Autowired
    private KafkaEsStoreListener esStoreListener;
    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;


    @Configuration
    static class TestConfig {
        @Bean
        public KafkaDbStoreListener dbStoreListener() {
            return new KafkaDbStoreListener();
        }

        @Bean
        public KafkaEsStoreListener esStoreListener() {
            return new KafkaEsStoreListener();
        }
    }


    public static class KafkaDbStoreListener {
        AtomicInteger counter = new AtomicInteger(0);
        AtomicLong total = new AtomicLong(0);

        @KafkaListener(groupId = "KafkaStreamsTest", topics = "db-store-topic", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.info("KafkaDbStoreListener : {}", payload);
            SalesEvent sales = JsonMapper.readFromJson(payload, SalesEvent.class);
            total.addAndGet(sales.getSalesAmount());

            counter.incrementAndGet();
        }
    }

    public static class KafkaEsStoreListener {
        AtomicInteger counter = new AtomicInteger(0);
        AtomicLong total = new AtomicLong(0);

        @KafkaListener(groupId = "KafkaStreamsTest", topics = "es-store-topic", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.info("KafkaEsStoreListener : {}", payload);
            SalesEvent sales = JsonMapper.readFromJson(payload, SalesEvent.class);
            total.addAndGet(sales.getSalesAmount());
            counter.incrementAndGet();
        }
    }

    @BeforeEach
    public void setUp() {
        this.registry.getListenerContainers().stream().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, this.embeddedKafkaBroker.getPartitionsPerTopic()));
    }

    @AfterEach
    public void tearDown() {
        // for clean up state store
        this.streamsBuilderFactoryBean.getKafkaStreams().close();
        this.streamsBuilderFactoryBean.getKafkaStreams().cleanUp();
    }

    @Test
    public void testKafkaStreams() throws Exception {
        SalesEvent event1 = Util.buildSalesEvent("event1",
                "CASH",
                10000L,
                "DB");
        this.sendMessage(SALES_TOPIC, event1);

        SalesEvent event2 = Util.buildSalesEvent("event2",
                "CASH",
                20000L,
                "ES");
        this.sendMessage(SALES_TOPIC, event2);

        // will transform fee
        SalesEvent event3 = Util.buildSalesEvent("event3",
                "CARD",
                30000L,
                "DB");
        this.sendMessage(SALES_TOPIC, event3);

        SalesEvent event4 = Util.buildSalesEvent("event4",
                "CARD",
                40000L,
                "ES");
        this.sendMessage(SALES_TOPIC, event4);

        // unsupported data store
        SalesEvent event5 = Util.buildSalesEvent("event5",
                "CARD",
                10000L,
                "REDIS");
        this.sendMessage(SALES_TOPIC, event5);

        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.dbStoreListener.counter::get, is(2));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.esStoreListener.counter::get, is(2));

        assertThat(this.dbStoreListener.total.get(), is(event1.getSalesAmount() + event3.transformCardFee()));
        assertThat(this.esStoreListener.total.get(), is(event2.getSalesAmount() + event4.transformCardFee()));

        ReadOnlyKeyValueStore<String, Long> totalAmount = this.streamsBuilderFactoryBean.getKafkaStreams()
                .store(
                        StoreQueryParameters.fromNameAndType("totalAmount", QueryableStoreTypes.keyValueStore())
                );
        assertThat(totalAmount.get("CASH"), is(event1.getSalesAmount() + event2.getSalesAmount()));
        assertThat(totalAmount.get("CARD"), is(event3.transformCardFee() + event4.transformCardFee()));
    }

    @Test
    public void testTopology() {
        ResponseEntity<String> topology = this.testRestTemplate.getForEntity("/v1/kafka-streams/topology/", String.class);
        assertThat(topology.getStatusCode(), is(HttpStatus.OK));
        assertThat(topology.getBody(), allOf(
                containsString("topics: [sales-topic]"),
                containsString("Processor: sales-type-"),
                containsString("Processor: data-store-"),
                containsString("topic: db-store-topic"),
                containsString("topic: es-store-topic")));

        log.info(topology.getBody());
    }

    private SendResult<String, String> sendMessage(String topic, SalesEvent event) throws Exception{
        String payload = JsonMapper.writeToJson(event);
        List<Header> headers = new ArrayList<>();
        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, event.getEventId(), payload, headers);

        final SendResult<String, String> result = (SendResult<String, String>) this.testKafkaTemplate.send(record).get();
        final RecordMetadata metadata = result.getRecordMetadata();

        log.debug(String.format("Sent record(key=%s value=%s) meta(topic=%s, partition=%d, offset=%d)",
                record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset()));

        return result;
    }
}
