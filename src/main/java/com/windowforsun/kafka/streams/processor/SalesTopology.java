package com.windowforsun.kafka.streams.processor;

import com.windowforsun.kafka.streams.event.SalesEvent;
import com.windowforsun.kafka.streams.properties.DemoProperties;
import com.windowforsun.kafka.streams.serdes.SalesSerdes;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class SalesTopology {
    private final DemoProperties properties;
    private static List<String> SUPPORTED_DATA_STORE = Arrays.stream(DataStore.values()).map(Enum::name).collect(Collectors.toList());
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, SalesEvent> messageStreams = streamsBuilder
                // subscribe inbound topic
                .stream(this.properties.getSalesInboundTopic(), Consumed.with(STRING_SERDE, SalesSerdes.serdes()))
                .peek((k, sales) -> log.info("salesInbound {} : {}", k, sales))
                // filtering unsupported data store
                .filter((k, sales) -> SUPPORTED_DATA_STORE.contains(sales.getDataStore()))
                .peek((k, sales) -> log.info("salesInbound filtered {} : {}", k, sales));

        // branching by sales type cash or card
        Map<String, KStream<String, SalesEvent>> salesTypeBranches = messageStreams.split(Named.as("sales-type-"))
                .branch((k, sales) -> sales.getSalesType().toUpperCase().equals(SalesType.CASH.name()), Branched.as("cash"))
                .branch((k, sales) -> sales.getSalesType().toUpperCase().equals(SalesType.CARD.name()), Branched.as("card"))
                .noDefaultBranch();

        // card type charging card fee
        KStream<String, SalesEvent> feeStreams = salesTypeBranches.get("sales-type-card")
                .mapValues(sales -> SalesEvent.builder()
                        .eventId(sales.getEventId())
                        // calculate charging card fee
                        .salesAmount(sales.transformCardFee())
                        .salesType(sales.getSalesType())
                        .dataStore(sales.getDataStore())
                        .build());

        // merging card and cash branched streams
        KStream<String, SalesEvent> mergedStreams = salesTypeBranches.get("sales-type-cash")
                .merge(feeStreams)
                .peek((k, sales) -> log.info("merged sales {} : {}", k, sales));

        // aggregating total sales amounts by sales type in state store(rocksdb)
        mergedStreams
                .map((k, sales) -> new KeyValue<>(sales.getSalesType(), sales.getSalesAmount()))
                .groupByKey(Grouped.with(STRING_SERDE, LONG_SERDE))
                .aggregate(() -> 0L,
                        (k, v, agg) -> agg + v,
                        Materialized.<String, Long, KeyValueStore< Bytes, byte[]>>as("totalAmount")
                                .withKeySerde(STRING_SERDE)
                                .withValueSerde(LONG_SERDE)
                );

        // branching by data store db or es
        Map<String, KStream<String, SalesEvent>> dataStoreBranches = mergedStreams
                .split(Named.as("data-store-"))
                .branch((k, sales) -> sales.getDataStore().equals(DataStore.DB.name()), Branched.as("db"))
                .branch((k, sales) -> sales.getDataStore().equals(DataStore.ES.name()), Branched.as("es"))
                .noDefaultBranch();

        // send to db store outbound topic
        dataStoreBranches.get("data-store-db").to(this.properties.getDbStoreOutboundTopic(), Produced.with(STRING_SERDE, SalesSerdes.serdes()));
        // send to es store outbound topic
        dataStoreBranches.get("data-store-es").to(this.properties.getEsStoreOutboundTopic(), Produced.with(STRING_SERDE, SalesSerdes.serdes()));
    }
}
