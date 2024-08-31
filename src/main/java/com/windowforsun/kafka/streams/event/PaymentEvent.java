package com.windowforsun.kafka.streams.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {
    private String paymentId;
    private Long amount;
    private String currency;
    private String toAccount;
    private String fromAccount;
    private String rails;
}
