package com.windowforsun.kafka.streams;

import com.windowforsun.kafka.streams.event.PaymentEvent;
import com.windowforsun.kafka.streams.event.SalesEvent;

public class Util {

    public static PaymentEvent buildPaymentEvent(String id, Long amount, String currency, String fromAccount, String toAccount, String rails) {
        return PaymentEvent.builder()
                .paymentId(id)
                .amount(amount)
                .currency(currency)
                .fromAccount(fromAccount)
                .toAccount(toAccount)
                .rails(rails)
                .build();
    }

    public static SalesEvent buildSalesEvent(String id, String salesType, Long salesAmount, String dataStore) {
        return SalesEvent.builder()
                .eventId(id)
                .salesType(salesType)
                .salesAmount(salesAmount)
                .dataStore(dataStore)
                .build();
    }
}
