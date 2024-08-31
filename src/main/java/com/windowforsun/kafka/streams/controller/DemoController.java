package com.windowforsun.kafka.streams.controller;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/kafka-streams")
@RequiredArgsConstructor
public class DemoController {
//    @Autowired
    private final StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/topology")
    public ResponseEntity<String> getTopology() {
        return ResponseEntity.ok(this.factoryBean.getTopology().describe().toString());
    }

    @GetMapping("/balance/{account}")
    public ResponseEntity<Long> getAccountBalance(@PathVariable String account) {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        ReadOnlyKeyValueStore<String, Long> balances = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("balance", QueryableStoreTypes.keyValueStore())
        );

        ResponseEntity<Long> response;

        if(balances.get(account) == null) {
            response = ResponseEntity.notFound().build();
        } else {
            response = ResponseEntity.ok(balances.get(account));
        }

        return response;
    }
}
