/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.asyncprocessing.declare;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.Deque;
import java.util.LinkedList;

/**
 * A declaration chain allows to declare multiple async operations in a single chain.
 *
 * @param <IN> The type of the input elements.
 */
@Experimental
public class DeclarationChain<IN> implements ThrowingConsumer<IN, Exception> {

    private final DeclarationContext context;

    private final Deque<Transformation<?, ?>> transformations;

    private DeclarationStage<?> currentStage;

    DeclarationChain(DeclarationContext context) {
        this.context = context;
        this.transformations = new LinkedList<>();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(IN in) throws Exception {
        StateFuture future = StateFutureUtils.completedFuture(in);
        for (Transformation trans : transformations) {
            future = trans.apply(future);
        }
    }

    public DeclarationStage<IN> firstStage() throws DeclarationException {
        if (currentStage != null) {
            throw new DeclarationException(
                    "Diverged declaration. Please make sure you call firstStage() once.");
        }
        DeclarationStage<IN> declarationStage = new DeclarationStage<>();
        currentStage = declarationStage;
        return declarationStage;
    }

    Transformation<?, ?> getLastTransformation() {
        return transformations.getLast();
    }

    /**
     * A DeclarationStage is a single stage in a declaration chain. It allows a further chaining of
     * operations.
     *
     * @param <T> The output of previous transformations. Will be the input type of further chained
     *     operation.
     */
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
            ComposeTransformation<T, U> trans = new ComposeTransformation<>(action);
            transformations.add(trans);
            currentStage = next;
            getLastTransformation().declare();
            return next;
        }

        public DeclarationStage<Void> thenAccept(ThrowingConsumer<T, Exception> action)
                throws DeclarationException {
            preCheck();
            DeclarationStage<Void> next = new DeclarationStage<>();
            AcceptTransformation<T> trans = new AcceptTransformation<>(action);
            transformations.add(trans);
            currentStage = next;
            getLastTransformation().declare();
            return next;
        }

        public DeclarationStage<T> withName(String name) throws DeclarationException {
            getLastTransformation().withName(name);
            return this;
        }

        public DeclarationChain<IN> finish() throws DeclarationException {
            preCheck();
            getLastTransformation().declare();
            return DeclarationChain.this;
        }
    }

    @Internal
    interface Transformation<FROM, TO> {
        StateFuture<TO> apply(StateFuture<FROM> upstream) throws Exception;

        void withName(String name) throws DeclarationException;

        void declare() throws DeclarationException;
    }

    private abstract static class AbstractTransformation<FROM, TO>
            implements Transformation<FROM, TO> {

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

    private class ComposeTransformation<FROM, TO> extends AbstractTransformation<FROM, TO> {

        FunctionWithException<FROM, StateFuture<TO>, ? extends Exception> action;

        NamedFunction<FROM, StateFuture<TO>> namedFunction;

        ComposeTransformation(FunctionWithException<FROM, StateFuture<TO>, Exception> action) {
            this.action = action;
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

    private class AcceptTransformation<FROM> extends AbstractTransformation<FROM, Void> {

        ThrowingConsumer<FROM, Exception> action;

        NamedConsumer<FROM> namedFunction;

        AcceptTransformation(ThrowingConsumer<FROM, Exception> action) {
            this.action = action;
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
