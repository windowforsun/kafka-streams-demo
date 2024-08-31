package com.windowforsun.kafka.streams.processor;

import com.windowforsun.kafka.streams.Util;
import com.windowforsun.kafka.streams.event.PaymentEvent;
import com.windowforsun.kafka.streams.properties.DemoProperties;
import com.windowforsun.kafka.streams.serdes.PaymentSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class PaymentTopologyTest {
    private PaymentTopology paymentTopology;
    private DemoProperties properties;

    private static final String PAYMENT_INBOUND_TOPIC = "payment-topic";
    private static final String RAILS_FOO_OUTBOUND_TOPIC = "rails-foo-topic";
    private static final String RAILS_BAR_OUTBOUND_TOPIC = "rails-bar-topic";

    private static final String ACCOUNT_GBP_ABC = "ABC-" + UUID.randomUUID();
    private static final String ACCOUNT_GBP_DEF = "DEF-" + UUID.randomUUID();

    private static final String ACCOUNT_USD_XYZ = "XYZ-" + UUID.randomUUID();

    @BeforeEach
    public void setUp() {
        this.properties = mock(DemoProperties.class);
        when(this.properties.getPaymentInboundTopic()).thenReturn(PAYMENT_INBOUND_TOPIC);
        when(this.properties.getRailsBarOutboundTopic()).thenReturn(RAILS_BAR_OUTBOUND_TOPIC);
        when(this.properties.getRailsFooOutboundTopic()).thenReturn(RAILS_FOO_OUTBOUND_TOPIC);

        this.paymentTopology = new PaymentTopology(this.properties);
    }

    @Test
    public void testPaymentTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        this.paymentTopology.buildPipeline(streamsBuilder);
        Topology topology = streamsBuilder.build();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration);
        TestInputTopic<String, PaymentEvent> inputTopic = topologyTestDriver
                .createInputTopic(PAYMENT_INBOUND_TOPIC, new StringSerializer(), PaymentSerdes.serdes().serializer());
        TestOutputTopic<String, PaymentEvent> railsFooOutputTopic = topologyTestDriver
                .createOutputTopic(RAILS_FOO_OUTBOUND_TOPIC, new StringDeserializer(), PaymentSerdes.serdes().deserializer());
        TestOutputTopic<String, PaymentEvent> railsBarOutputTopic = topologyTestDriver
                .createOutputTopic(RAILS_BAR_OUTBOUND_TOPIC, new StringDeserializer(), PaymentSerdes.serdes().deserializer());

        PaymentEvent payment1 = Util.buildPaymentEvent("payment1",
                100L,
                "GBP",
                ACCOUNT_GBP_ABC,
                ACCOUNT_GBP_DEF,
                Rails.BANK_RAILS_FOO.name());
        inputTopic.pipeInput(payment1.getPaymentId(), payment1);

        PaymentEvent payment2 = Util.buildPaymentEvent("payment2",
                50L,
                "GBP",
                ACCOUNT_GBP_ABC,
                ACCOUNT_GBP_DEF,
                Rails.BANK_RAILS_FOO.name());
        inputTopic.pipeInput(payment2.getPaymentId(), payment2);

        PaymentEvent payment3 = Util.buildPaymentEvent("payment3",
                60L,
                "GBP",
                ACCOUNT_GBP_ABC,
                ACCOUNT_GBP_DEF,
                Rails.BANK_RAILS_FOO.name());
        inputTopic.pipeInput(payment3.getPaymentId(), payment3);

        // unsupported rails
        PaymentEvent payment4 = Util.buildPaymentEvent("payment4",
                1200L,
                "GBP",
                ACCOUNT_GBP_ABC,
                ACCOUNT_GBP_DEF,
                Rails.BANK_RAILS_XXX.name());
        inputTopic.pipeInput(payment4.getPaymentId(), payment4);

        // usd will fx
        PaymentEvent payment5 = Util.buildPaymentEvent("payment5",
                1000L,
                "USD",
                ACCOUNT_USD_XYZ,
                ACCOUNT_GBP_DEF,
                Rails.BANK_RAILS_BAR.name());
        inputTopic.pipeInput(payment5.getPaymentId(), payment5);

        // check rails foo topic, gbp
        List<KeyValue<String, PaymentEvent>> fooList = railsFooOutputTopic.readKeyValuesToList();
        assertThat(fooList,
                hasItems(
                        KeyValue.pair(payment1.getPaymentId(), payment1),
                        KeyValue.pair(payment2.getPaymentId(), payment2),
                        KeyValue.pair(payment3.getPaymentId(), payment3)
                ));

        // check rails bar topic, usd fx
        PaymentEvent payment5Fx = Util.buildPaymentEvent(payment5.getPaymentId(),
                800L,
                "GBP",
                payment5.getFromAccount(),
                payment5.getToAccount(),
                payment5.getRails());
        List<KeyValue<String, PaymentEvent>> barList = railsBarOutputTopic.readKeyValuesToList();
        assertThat(barList,
                hasItems(KeyValue.pair(payment5Fx.getPaymentId(), payment5Fx)));

        // check account store
        KeyValueStore<String, Long> balanceStore = topologyTestDriver.getKeyValueStore("balance");
        assertThat(balanceStore.get(ACCOUNT_GBP_ABC), is(210L));
        assertThat(balanceStore.get(ACCOUNT_GBP_DEF), nullValue());
        assertThat(balanceStore.get(ACCOUNT_USD_XYZ), is(800L));
    }
}
