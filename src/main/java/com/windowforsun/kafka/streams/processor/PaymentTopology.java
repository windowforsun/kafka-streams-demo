package com.windowforsun.kafka.streams.processor;

import com.windowforsun.kafka.streams.event.PaymentEvent;
import com.windowforsun.kafka.streams.properties.DemoProperties;
import com.windowforsun.kafka.streams.serdes.PaymentSerdes;
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

import java.util.List;
import java.util.Map;

@Slf4j
//@Component
@RequiredArgsConstructor
public class PaymentTopology {
    private final DemoProperties properties;
    private static List<String> SUPPORTED_RAILS = List.of(Rails.BANK_RAILS_BAR.name(), Rails.BANK_RAILS_FOO.name());
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, PaymentEvent> messageStreams = streamsBuilder
                .stream(this.properties.getPaymentInboundTopic(), Consumed.with(STRING_SERDE, PaymentSerdes.serdes()))
                .peek((k, p) -> log.info("paymentInboundTopic {} : {}", k, p))
                .filter((k, p) -> SUPPORTED_RAILS.contains(p.getRails()))
                .peek((k, p) -> log.info("filtered paymentInbound Topic {} : {}", k, p));

        Map<String, KStream<String, PaymentEvent>> currenciesBranches = messageStreams.split(Named.as("currency-"))
                .branch((k, p) -> p.getCurrency().equals(Currency.GBP.name()), Branched.as("gbp"))
                .branch((k, p) -> p.getCurrency().equals(Currency.USD.name()), Branched.as("usd"))
                .noDefaultBranch();


        System.out.println("test !! currenciesBranches " + currenciesBranches.size());
        currenciesBranches.forEach((s, stringPaymentEventKStream) -> System.out.println("test !! " + s));

//        KStream<String, PaymentEvent> fxStream = streamsBuilder.<String, PaymentEvent>stream("currency-usd").mapValues(payment -> {
        KStream<String, PaymentEvent> fxStream = currenciesBranches.get("currency-usd").mapValues(payment -> {
            double usdToGpbRate = 0.8;

            return PaymentEvent.builder()
                    .paymentId(payment.getPaymentId())
                    .amount(Math.round(payment.getAmount() * usdToGpbRate))
                    .currency(Currency.GBP.name())
                    .fromAccount(payment.getFromAccount())
                    .toAccount(payment.getToAccount())
                    .rails(payment.getRails())
                    .build();
        });

//        KStream<String, PaymentEvent> mergedStreams = streamsBuilder.<String, PaymentEvent>stream("currency-gbp").merge(fxStream)
        KStream<String, PaymentEvent> mergedStreams = currenciesBranches.get("currency-gbp").merge(fxStream)
                .peek((k , p) -> log.info("merge payment {} : {}", k, p));

        mergedStreams
                .map((k, p) -> new KeyValue<>(p.getFromAccount(), p.getAmount()))
                .groupByKey(Grouped.with(STRING_SERDE, LONG_SERDE))
                .aggregate(() -> 0L,
                        (key, value, aggregate) -> aggregate + value,
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("balance")
                                .withKeySerde(STRING_SERDE)
                                .withValueSerde(LONG_SERDE)
                        );

        Map<String, KStream<String, PaymentEvent>> railsBranches = mergedStreams.split(Named.as("rails-"))
                .branch((k, p) -> p.getRails().equals(Rails.BANK_RAILS_FOO.name()), Branched.as("foo"))
                .branch((k, p) -> p.getRails().equals(Rails.BANK_RAILS_BAR.name()), Branched.as("bar"))
                .noDefaultBranch();

//        streamsBuilder.<String, PaymentEvent>stream("rails-foo").to(this.properties.getRailsFooOutboundTopic(), Produced.with(STRING_SERDE, PaymentSerdes.serdes()));
        railsBranches.get("rails-foo").to(this.properties.getRailsFooOutboundTopic(), Produced.with(STRING_SERDE, PaymentSerdes.serdes()));
        railsBranches.get("rails-bar").to(this.properties.getRailsBarOutboundTopic(), Produced.with(STRING_SERDE, PaymentSerdes.serdes()));
//        streamsBuilder.<String, PaymentEvent>stream("rails-bar").to(this.properties.getRailsBarOutboundTopic(), Produced.with(STRING_SERDE, PaymentSerdes.serdes()));
    }
}
