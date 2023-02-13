package org.springframework.integration.samples.filesplit;


import com.google.auto.value.AutoValue;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.springframework.lang.Nullable;


@AutoValue
abstract class ContextAndScope {

    @Nullable
    abstract Context getContext();

    abstract Scope getScope();

    void close() {
        getScope().close();
    }

    static ContextAndScope create(Context context, Scope scope) {
        return new AutoValue_ContextAndScope(context, scope);
    }
}
