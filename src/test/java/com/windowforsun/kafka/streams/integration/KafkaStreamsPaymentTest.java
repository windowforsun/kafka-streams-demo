package com.windowforsun.kafka.streams.integration;

import com.windowforsun.kafka.streams.KafkaConfig;
import com.windowforsun.kafka.streams.Util;
import com.windowforsun.kafka.streams.event.PaymentEvent;
import com.windowforsun.kafka.streams.mapper.JsonMapper;
import com.windowforsun.kafka.streams.processor.Rails;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.awaitility.Awaitility;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = KafkaConfig.class)
@EmbeddedKafka(controlledShutdown = true, topics = {"payment-topic", "rails-foo-topic", "rails-bar-topic"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
public class KafkaStreamsPaymentTest {
    private final static String PAYMENT_TEST_TOPIC = "payment-topic";
    @Autowired
    private KafkaTemplate testKafkaTemplate;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private TestRestTemplate testRestTemplate;
    @Autowired
    private KafkaFooRailsListener fooRailsListener;
    @Autowired
    private KafkaBarRailsListener barRailsListener;

    // GBP Accounts.
    private static final String ACCOUNT_GBP_ABC = "ABC-" + UUID.randomUUID();
    private static final String ACCOUNT_GBP_DEF = "DEF-" + UUID.randomUUID();

    // USD Accounts.
    private static final String ACCOUNT_USD_XYZ = "XYZ-" + UUID.randomUUID();

    @Configuration
    static class TestConfig {
        @Bean
        public KafkaFooRailsListener fooRailsListener() {
            return new KafkaFooRailsListener();
        }

        @Bean
        public KafkaBarRailsListener barRailsListener() {
            return new KafkaBarRailsListener();
        }
    }

    public static class KafkaFooRailsListener {
        AtomicInteger counter = new AtomicInteger(0);
        AtomicLong total = new AtomicLong(0);

        @KafkaListener(groupId = "KafkaStreamsTest", topics = "rails-foo-topic", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.info("KafkaFooRailListener : {}", payload);
            PaymentEvent payment = JsonMapper.readFromJson(payload, PaymentEvent.class);
            total.addAndGet(payment.getAmount());

            counter.incrementAndGet();
        }
    }

    public static class KafkaBarRailsListener {
        AtomicInteger counter = new AtomicInteger(0);
        AtomicLong total = new AtomicLong(0);

        @KafkaListener(groupId = "KafkaStreamsTest", topics = "rails-bar-topic", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.info("KafkaBarRailsListener : {}", payload);
            PaymentEvent payment = JsonMapper.readFromJson(payload, PaymentEvent.class);
            total.addAndGet(payment.getAmount());
            counter.incrementAndGet();
        }
    }

    @BeforeEach
    public void setUp() {
        log.debug("test !! setUp start");
        this.registry.getListenerContainers().stream().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, this.embeddedKafkaBroker.getPartitionsPerTopic()));
        log.debug("test !! setUp end");
    }

    @Test
    public void testPaymentKafkaStreams() throws Exception {
        PaymentEvent payment1 = Util.buildPaymentEvent("payment1",
                100L,
                "GBP",
                ACCOUNT_GBP_ABC,
                ACCOUNT_GBP_DEF,
                Rails.BANK_RAILS_FOO.name());
        this.sendMessage(PAYMENT_TEST_TOPIC, payment1);

        PaymentEvent payment2 = Util.buildPaymentEvent("payment2",
                50L,
                "GBP",
                ACCOUNT_GBP_ABC,
                ACCOUNT_GBP_DEF,
                Rails.BANK_RAILS_FOO.name());
        this.sendMessage(PAYMENT_TEST_TOPIC, payment2);

        PaymentEvent payment3 = Util.buildPaymentEvent("payment3",
                60L,
                "GBP",
                ACCOUNT_GBP_ABC,
                ACCOUNT_GBP_DEF,
                Rails.BANK_RAILS_FOO.name());
        this.sendMessage(PAYMENT_TEST_TOPIC, payment3);

        // unsupported rails
        PaymentEvent payment4 = Util.buildPaymentEvent("payment5",
                1200L,
                "GBP",
                ACCOUNT_GBP_ABC,
                ACCOUNT_GBP_DEF,
                Rails.BANK_RAILS_XXX.name());
        this.sendMessage(PAYMENT_TEST_TOPIC, payment4);

        // check rails bar topic, usd fx
        PaymentEvent payment5 = Util.buildPaymentEvent("payment5",
                1000L,
                "USD",
                ACCOUNT_USD_XYZ,
                ACCOUNT_GBP_DEF,
                Rails.BANK_RAILS_BAR.name());
        this.sendMessage(PAYMENT_TEST_TOPIC, payment5);

        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.fooRailsListener.counter::get, is(3));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.barRailsListener.counter::get, is(1));

        assertThat(this.fooRailsListener.total.get(), is(210L));
        assertThat(this.barRailsListener.total.get(), is(800L));

        ResponseEntity<String> responseAbc = this.testRestTemplate.getForEntity("/v1/kafka-streams/balance/" + ACCOUNT_GBP_ABC, String.class);
        assertThat(responseAbc.getStatusCode(), is(HttpStatus.OK));
        assertThat(responseAbc.getBody(), is("210"));

        ResponseEntity<String> responseDef = this.testRestTemplate.getForEntity("/v1/kafka-streams/balance/" + ACCOUNT_GBP_DEF, String.class);
        assertThat(responseDef.getStatusCode(), is(HttpStatus.NOT_FOUND));

        ResponseEntity<String> responseXyz = this.testRestTemplate.getForEntity("/v1/kafka-streams/balance/" + ACCOUNT_USD_XYZ, String.class);
        assertThat(responseXyz.getStatusCode(), is(HttpStatus.OK));
        assertThat(responseXyz.getBody(), is("800"));
    }

    @Test
    public void testTopology() throws Exception {
        ResponseEntity<String> topology = this.testRestTemplate.getForEntity("/v1/kafka-streams/topology/", String.class);
        assertThat(topology.getStatusCode(), is(HttpStatus.OK));
        assertThat(topology.getBody(), containsString("topics: [payment-topic]"));

        log.info(topology.getBody());
    }

    private SendResult<String, String> sendMessage(String topic, PaymentEvent event) throws Exception{
        log.debug("test !!");
        String payload = JsonMapper.writeToJson(event);
        List<Header> headers = new ArrayList<>();
        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, event.getPaymentId(), payload, headers);

        final SendResult<String, String> result = (SendResult<String, String>) this.testKafkaTemplate.send(record).get();
        final RecordMetadata metadata = result.getRecordMetadata();

        log.debug(String.format("Sent record(key=%s value=%s) meta(topic=%s, partition=%d, offset=%d)",
                record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset()));

        return result;
    }
}
