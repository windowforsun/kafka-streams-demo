package com.windowforsun.kafka.streams.serdes;

import com.windowforsun.kafka.streams.mapper.JsonMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {
    public JsonSerializer() {
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }

        try {
            return JsonMapper.writeToJson(data).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new SerializationException("Error serializing Json Message", e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }
}
