package org.springframework.integration.samples.filesplit;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.ExecutorChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.messaging.support.NativeMessageHeaderAccessor;
import org.springframework.util.LinkedMultiValueMap;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

class TracingChannelInterceptor implements ExecutorChannelInterceptor {
    private static final ThreadLocal<Map<Message<?>, ContextAndScope>> LOCAL_CONTEXT_AND_SCOPE =
            ThreadLocal.withInitial(IdentityHashMap::new);
    private final Logger logger = LoggerFactory.getLogger(TracingChannelInterceptor.class);

    private final TextMapGetter<MessageWithChannel> getter = MessageHeadersGetter.INSTANCE;
    private final TextMapSetter<MessageHeaderAccessor> setter = MessageHeadersSetter.INSTANCE;

    private final ContextPropagators propagators;
    private final Tracer tracer;

    TracingChannelInterceptor(OpenTelemetry otel) {
        propagators = otel.getPropagators();
        tracer = otel.getTracer("spring-integration-tracer");
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {

        // Recover context from upstream message
        MessageHeaderAccessor messageHeaderAccessor = createMutableHeaderAccessor(message);
        MessageWithChannel messageWithChannel = MessageWithChannel.create(message, channel);

        Context context = propagators.getTextMapPropagator().extract(Context.current(), messageWithChannel, getter);

        // Create new span
        Span span = tracer.spanBuilder(String.format("%s process", messageWithChannel.getChannelName()))
                .setParent(context)
                .setSpanKind(SpanKind.PRODUCER)
                .startSpan();
        context = context.with(span);
        Scope scope = span.makeCurrent();

        ContextAndScope contextAndScope = ContextAndScope.create(context, scope);

        // Inject context into the next message
        propagators.getTextMapPropagator()
                .inject(Context.current(), messageHeaderAccessor, MessageHeadersSetter.INSTANCE);
        message = createMessageWithHeaders(message, messageHeaderAccessor);

        Map<Message<?>, ContextAndScope> map = LOCAL_CONTEXT_AND_SCOPE.get();

        // Save context and span in thread.local
        map.put(message, contextAndScope);
        return message;
    }

    @Override
    public void afterSendCompletion(Message<?> message, MessageChannel channel, boolean sent, Exception ex) {

        // Restore context and span from thread.local
        Map<Message<?>, ContextAndScope> map = LOCAL_CONTEXT_AND_SCOPE.get();
        ContextAndScope contextAndScope = map.remove(message);

        // Close scope
        contextAndScope.close();

        // Restore span from context
        Span span = Span.fromContext(contextAndScope.getContext());

        // Register any exceptions
        if (ex != null) {
            span.setStatus(StatusCode.ERROR);
            span.recordException(ex);
        }

        // End of span
        span.end();
    }

    private static Message<?> createMessageWithHeaders(
            Message<?> message, MessageHeaderAccessor messageHeaderAccessor) {
        return MessageBuilder.fromMessage(message)
                .copyHeaders(messageHeaderAccessor.toMessageHeaders())
                .build();
    }

    private static MessageHeaderAccessor createMutableHeaderAccessor(Message<?> message) {
        MessageHeaderAccessor headerAccessor = MessageHeaderAccessor.getMutableAccessor(message);
        headerAccessor.setLeaveMutable(true);
        ensureNativeHeadersAreMutable(headerAccessor);
        return headerAccessor;
    }

    private static void ensureNativeHeadersAreMutable(MessageHeaderAccessor headerAccessor) {
        Object nativeMap = headerAccessor.getHeader(NativeMessageHeaderAccessor.NATIVE_HEADERS);
        if (nativeMap != null && !(nativeMap instanceof LinkedMultiValueMap)) {
            @SuppressWarnings("unchecked")
            Map<String, List<String>> map = (Map<String, List<String>>) nativeMap;
            headerAccessor.setHeader(
                    NativeMessageHeaderAccessor.NATIVE_HEADERS, new LinkedMultiValueMap<>(map));
        }
    }
}
