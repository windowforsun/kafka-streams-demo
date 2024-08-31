package com.windowforsun.kafka.streams.serdes;

import com.windowforsun.kafka.streams.event.PaymentEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class PaymentSerdes extends Serdes.WrapperSerde<PaymentSerdes> {
    public PaymentSerdes() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(PaymentSerdes.class));
    }

    public static Serde<PaymentEvent> serdes() {
        JsonSerializer<PaymentEvent> serializer = new JsonSerializer<>();
        JsonDeserializer<PaymentEvent> deserializer = new JsonDeserializer<>(PaymentEvent.class);

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
