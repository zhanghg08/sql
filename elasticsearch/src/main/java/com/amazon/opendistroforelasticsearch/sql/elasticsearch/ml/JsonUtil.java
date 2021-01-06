package com.amazon.opendistroforelasticsearch.sql.elasticsearch.ml;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;

@UtilityClass
public class JsonUtil {
    private final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
    }

    public String serialize(Object object) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> {
            try {
                return objectMapper.writeValueAsString(object);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("json serialization exception", e);
            }
        });
    }

    public <T> T deserialize(String jsonData, Class<T> t) {
        try {
            return objectMapper.readValue(jsonData, t);
        } catch (IOException e) {
            throw new RuntimeException("json deserialization exception", e);
        }
    }

    public <T> T deserialize(String jsonData, TypeReference<T> t) {
        return AccessController.doPrivileged((PrivilegedAction<T>) () -> {
            try {
                return objectMapper.readValue(jsonData, t);
            } catch (IOException e) {
                throw new RuntimeException("json deserialization exception", e);
            }
        });
    }
}
