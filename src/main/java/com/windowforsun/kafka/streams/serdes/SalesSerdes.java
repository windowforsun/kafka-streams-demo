package com.windowforsun.kafka.streams.serdes;

import com.windowforsun.kafka.streams.event.SalesEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SalesSerdes extends Serdes.WrapperSerde<SalesEvent> {
    public SalesSerdes() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(SalesEvent.class));
    }

    public static Serde<SalesEvent> serdes() {
        JsonSerializer<SalesEvent> serializer = new JsonSerializer<>();
        JsonDeserializer<SalesEvent> deserializer = new JsonDeserializer<>(SalesEvent.class);

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
