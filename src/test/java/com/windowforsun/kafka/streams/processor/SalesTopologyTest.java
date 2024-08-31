package com.windowforsun.kafka.streams.processor;

import com.windowforsun.kafka.streams.Util;
import com.windowforsun.kafka.streams.event.SalesEvent;
import com.windowforsun.kafka.streams.properties.DemoProperties;
import com.windowforsun.kafka.streams.serdes.SalesSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class SalesTopologyTest {
    private SalesTopology salesTopology;
    private DemoProperties properties;
    private static final String SALES_INBOUND_TOPIC = "sales-topic";
    private static final String DB_STORE_OUTBOUND_TOPIC = "db-topic";
    private static final String ES_STORE_OUTBOUND_TOPIC = "es-topic";
    private StreamsBuilder streamsBuilder;
    private Topology topology;
    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, SalesEvent> salesInputTopic;
    private TestOutputTopic<String, SalesEvent> dbStoreOutputTopic;
    private TestOutputTopic<String, SalesEvent> esStoreOutputTopic;


    @BeforeEach
    public void setUp() {
        this.properties = mock(DemoProperties.class);
        when(this.properties.getSalesInboundTopic()).thenReturn(SALES_INBOUND_TOPIC);
        when(this.properties.getDbStoreOutboundTopic()).thenReturn(DB_STORE_OUTBOUND_TOPIC);
        when(this.properties.getEsStoreOutboundTopic()).thenReturn(ES_STORE_OUTBOUND_TOPIC);

        this.salesTopology = new SalesTopology(this.properties);
        this.streamsBuilder = new StreamsBuilder();
        this.salesTopology.buildPipeline(this.streamsBuilder);
        this.topology = streamsBuilder.build();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        this.topologyTestDriver = new TopologyTestDriver(this.topology, streamsConfiguration);
        this.salesInputTopic = this.topologyTestDriver.createInputTopic(SALES_INBOUND_TOPIC, new StringSerializer(), SalesSerdes.serdes().serializer());
        this.dbStoreOutputTopic = this.topologyTestDriver.createOutputTopic(DB_STORE_OUTBOUND_TOPIC, new StringDeserializer(), SalesSerdes.serdes().deserializer());
        this.esStoreOutputTopic = this.topologyTestDriver.createOutputTopic(ES_STORE_OUTBOUND_TOPIC, new StringDeserializer(), SalesSerdes.serdes().deserializer());
    }

    @Test
    public void testTopology() {
        SalesEvent event1 = Util.buildSalesEvent("event1",
                "CASH",
                10000L,
                "DB");
        this.salesInputTopic.pipeInput(event1.getEventId(), event1);

        SalesEvent event2 = Util.buildSalesEvent("event2",
                "CASH",
                20000L,
                "ES");
        this.salesInputTopic.pipeInput(event2.getEventId(), event2);

        // will transform fee
        SalesEvent event3 = Util.buildSalesEvent("event3",
                "CARD",
                30000L,
                "DB");
        this.salesInputTopic.pipeInput(event3.getEventId(), event3);

        SalesEvent event4 = Util.buildSalesEvent("event4",
                "CARD",
                40000L,
                "ES");
        this.salesInputTopic.pipeInput(event4.getEventId(), event4);

        // unsupported data store
        SalesEvent event5 = Util.buildSalesEvent("event5",
                "CARD",
                10000L,
                "REDIS");
        this.salesInputTopic.pipeInput(event5.getEventId(), event5);



        List<KeyValue<String, SalesEvent>> dbStoreList = this.dbStoreOutputTopic.readKeyValuesToList();
        SalesEvent event3Fee = Util.buildSalesEvent("event3",
                "CARD",
                event3.transformCardFee(),
                "DB");
        assertThat(dbStoreList,
                containsInAnyOrder(
                        KeyValue.pair(event1.getEventId(), event1),
                        KeyValue.pair(event3Fee.getEventId(), event3Fee)
                ));

        List<KeyValue<String, SalesEvent>> esStoreList = this.esStoreOutputTopic.readKeyValuesToList();
        SalesEvent event4fee = Util.buildSalesEvent("event4",
                "CARD",
                event4.transformCardFee(),
                "ES");
        assertThat(esStoreList,
                hasItems(
                        KeyValue.pair(event2.getEventId(), event2),
                        KeyValue.pair(event4fee.getEventId(), event4fee)
                ));
    }


}
