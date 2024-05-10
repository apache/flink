package org.apache.flink.streaming.runtime.operators.asyncprocessing.declare;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.Deque;
import java.util.LinkedList;

public class DeclarationChain<IN, FIRST> implements ThrowingConsumer<IN, Exception> {

    private final DeclarationContext context;

    private final FunctionWithException<IN, StateFuture<FIRST>, Exception> first;

    private final Deque<Transformation<?, ?>> transformations;

    private DeclarationStage<?> currentStage;

    DeclarationChain(
            DeclarationContext context,
            FunctionWithException<IN, StateFuture<FIRST>, Exception> first) {
        this.context = context;
        this.first = first;
        this.transformations = new LinkedList<>();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(IN in) throws Exception {
        StateFuture future = first.apply(in);
        for (Transformation trans : transformations) {
            future = trans.apply(future);
        }
    }

    public DeclarationStage<FIRST> firstStage() throws DeclarationException {
        if (currentStage != null) {
            throw new DeclarationException(
                    "Diverged declaration. Please make sure you call firstStage() once.");
        }
        DeclarationStage<FIRST> declarationStage = new DeclarationStage<>();
        currentStage = declarationStage;
        return declarationStage;
    }

    Transformation<?, ?> getLastTransformation() {
        return transformations.getLast();
    }

    public class DeclarationStage<T> {

        private boolean afterThen = false;

        private void preCheck() throws DeclarationException {
            if (afterThen) {
                throw new DeclarationException(
                        "Double thenCompose called for single declaration block.");
            }
            if (currentStage != this) {
                throw new DeclarationException(
                        "Diverged declaration. Please make sure you are declaring on the last point.");
            }
            afterThen = true;
        }

        public <U> DeclarationStage<U> thenCompose(
                FunctionWithException<T, StateFuture<U>, Exception> action)
                throws DeclarationException {
            preCheck();
            DeclarationStage<U> next = new DeclarationStage<>();
            ComposeTransformation<T, U> trans = new ComposeTransformation<>(action, next);
            transformations.add(trans);
            currentStage = next;
            getLastTransformation().declare();
            return next;
        }

        public DeclarationStage<Void> thenAccept(ThrowingConsumer<T, Exception> action)
                throws DeclarationException {
            preCheck();
            DeclarationStage<Void> next = new DeclarationStage<>();
            AcceptTransformation<T> trans = new AcceptTransformation<>(action, next);
            transformations.add(trans);
            currentStage = next;
            getLastTransformation().declare();
            return next;
        }

        public DeclarationStage<T> withName(String name) throws DeclarationException {
            getLastTransformation().withName(name);
            return this;
        }

        public DeclarationChain<IN, FIRST> finish() throws DeclarationException {
            preCheck();
            getLastTransformation().declare();
            return DeclarationChain.this;
        }
    }

    interface Transformation<FROM, TO> {
        StateFuture<TO> apply(StateFuture<FROM> upstream) throws Exception;

        void withName(String name) throws DeclarationException;

        void declare() throws DeclarationException;
    }

    abstract static class AbstractTransformation<FROM, TO> implements Transformation<FROM, TO> {

        String name = null;

        @Override
        public void withName(String newName) throws DeclarationException {
            if (name != null) {
                throw new DeclarationException("Double naming");
            }
            name = newName;
            declare();
        }
    }

    class ComposeTransformation<FROM, TO> extends AbstractTransformation<FROM, TO> {

        DeclarationStage<TO> to;

        FunctionWithException<FROM, StateFuture<TO>, ? extends Exception> action;

        NamedFunction<FROM, StateFuture<TO>> namedFunction;

        ComposeTransformation(
                FunctionWithException<FROM, StateFuture<TO>, Exception> action,
                DeclarationStage<TO> to) {
            this.action = action;
            this.to = to;
        }

        @Override
        public StateFuture<TO> apply(StateFuture<FROM> upstream) throws Exception {
            return upstream.thenCompose(namedFunction);
        }

        @Override
        public void declare() throws DeclarationException {
            if (namedFunction == null) {
                if (name == null) {
                    namedFunction = context.declare(action);
                } else {
                    namedFunction = context.declare(name, action);
                }
            }
        }
    }

    class AcceptTransformation<FROM> extends AbstractTransformation<FROM, Void> {

        DeclarationStage<Void> to;

        ThrowingConsumer<FROM, Exception> action;

        NamedConsumer<FROM> namedFunction;

        AcceptTransformation(ThrowingConsumer<FROM, Exception> action, DeclarationStage<Void> to) {
            this.action = action;
            this.to = to;
        }

        @Override
        public StateFuture<Void> apply(StateFuture<FROM> upstream) throws Exception {
            return upstream.thenAccept(namedFunction);
        }

        @Override
        public void declare() throws DeclarationException {
            if (namedFunction == null) {
                if (name == null) {
                    namedFunction = context.declare(action);
                } else {
                    namedFunction = context.declare(name, action);
                }
            }
        }
    }
}
