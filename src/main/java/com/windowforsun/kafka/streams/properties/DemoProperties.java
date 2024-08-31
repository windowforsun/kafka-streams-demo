package com.windowforsun.kafka.streams.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;

@Configuration
@ConfigurationProperties("demo")
@Getter
@Setter
@Validated
public class DemoProperties {
    @NotNull
    private String id;
    @NotNull
    private String paymentInboundTopic;
    @NotNull
    private String railsFooOutboundTopic;
    @NotNull
    private String railsBarOutboundTopic;

    @NotNull
    private String salesInboundTopic;
    @NotNull
    private String dbStoreOutboundTopic;
    @NotNull
    private String esStoreOutboundTopic;
}
