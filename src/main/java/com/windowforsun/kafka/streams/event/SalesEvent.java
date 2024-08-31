package com.windowforsun.kafka.streams.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SalesEvent {
    private String eventId;
    private String salesType;
    private Long salesAmount;
    private String dataStore;

    public Long transformCardFee() {
        double cardFeeRate = 0.0025;

        return this.salesAmount - Math.round(this.salesAmount * cardFeeRate);
    }
}
