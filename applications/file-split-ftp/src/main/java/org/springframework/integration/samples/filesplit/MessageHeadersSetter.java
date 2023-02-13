package org.springframework.integration.samples.filesplit;

import io.opentelemetry.context.propagation.TextMapSetter;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.messaging.support.NativeMessageHeaderAccessor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

enum MessageHeadersSetter implements TextMapSetter<MessageHeaderAccessor> {
    INSTANCE;

    @Override
    public void set(MessageHeaderAccessor carrier, String key, String value) {
        carrier.setHeader(key, value);
        setNativeHeader(carrier, key, value);
    }

    private static void setNativeHeader(MessageHeaderAccessor carrier, String key, String value) {
        Object nativeMap = carrier.getHeader(NativeMessageHeaderAccessor.NATIVE_HEADERS);
        if (nativeMap instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, List<String>> map = ((Map<String, List<String>>) nativeMap);
            map.put(key, Collections.singletonList(value));
        }
    }
}