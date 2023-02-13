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
    public Message<?> beforeHandle(Message<?> message, MessageChannel channel, MessageHandler handler) {
        logger.info("**************** beforeHandle " + message.getHeaders());
        return message;
    }

    @Override
    public void afterMessageHandled(Message<?> message, MessageChannel channel, MessageHandler handler, Exception ex) {
        logger.info("****************** afterMessageHandled " + message.getHeaders());
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        logger.info("preSend " + message.getHeaders());
        MessageHeaderAccessor messageHeaderAccessor = createMutableHeaderAccessor(message);
        MessageWithChannel messageWithChannel = MessageWithChannel.create(message, channel);

        Context context = propagators.getTextMapPropagator().extract(Context.current(), messageWithChannel, getter);

        Span span = tracer.spanBuilder(String.format("%s process", messageWithChannel.getChannelName()))
                .setParent(context)
                .setSpanKind(SpanKind.PRODUCER)
                .startSpan();

        context = context.with(span);
        Scope scope = span.makeCurrent();

        ContextAndScope contextAndScope = ContextAndScope.create(context, scope);

        propagators.getTextMapPropagator()
                .inject(Context.current(), messageHeaderAccessor, MessageHeadersSetter.INSTANCE);
        message = createMessageWithHeaders(message, messageHeaderAccessor);

        Map<Message<?>, ContextAndScope> map = LOCAL_CONTEXT_AND_SCOPE.get();
        map.put(message, contextAndScope);
        return message;
    }

    @Override
    public void postSend(Message<?> message, MessageChannel channel, boolean sent) {

        logger.info("postSend " + message.getHeaders());

    }

    @Override
    public void afterSendCompletion(Message<?> message, MessageChannel channel, boolean sent, Exception ex) {
        logger.info("afterSendCompletion " + message.getHeaders());

        Map<Message<?>, ContextAndScope> map = LOCAL_CONTEXT_AND_SCOPE.get();
        ContextAndScope contextAndScope = map.remove(message);

        contextAndScope.close();

        Span span = Span.fromContext(contextAndScope.getContext());

        if (ex != null) {
            span.setStatus(StatusCode.ERROR);
            span.recordException(ex);
        }

        span.end();
    }

    @Override
    public boolean preReceive(MessageChannel channel) {
        logger.info("preReceive " + channel);
        return ExecutorChannelInterceptor.super.preReceive(channel);
    }

    @Override
    public Message<?> postReceive(Message<?> message, MessageChannel channel) {
        logger.info("postReceive " + message.getHeaders());
        return message;
    }

    @Override
    public void afterReceiveCompletion(Message<?> message, MessageChannel channel, Exception ex) {
        if (message != null) {
            logger.info("afterReceiveCompletion " + message.getHeaders());
        }
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
