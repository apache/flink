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

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.Deque;
import java.util.LinkedList;

/**
 * A chain-style declaration that could execute in serial.
 *
 * @param <IN> The input type of this chain.
 * @param <FIRST> The output type of the first block of this chain.
 */
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
                FunctionWithException<T, StateFuture<U>, ? extends Exception> action)
                throws DeclarationException {
            preCheck();
            DeclarationStage<U> next = new DeclarationStage<>();
            ComposeTransformation<T, U> trans = new ComposeTransformation<>(action, next);
            transformations.add(trans);
            currentStage = next;
            getLastTransformation().declare();
            return next;
        }

        public DeclarationStage<Void> thenAccept(ThrowingConsumer<T, ? extends Exception> action)
                throws DeclarationException {
            preCheck();
            DeclarationStage<Void> next = new DeclarationStage<>();
            AcceptTransformation<T> trans = new AcceptTransformation<>(action, next);
            transformations.add(trans);
            currentStage = next;
            getLastTransformation().declare();
            return next;
        }

        public <U> DeclarationStage<U> thenApply(
                FunctionWithException<T, U, ? extends Exception> action)
                throws DeclarationException {
            preCheck();
            DeclarationStage<U> next = new DeclarationStage<>();
            ApplyTransformation<T, U> trans = new ApplyTransformation<>(action, next);
            transformations.add(trans);
            currentStage = next;
            getLastTransformation().declare();
            return next;
        }

        public <U, V> DeclarationStage<Tuple2<Boolean, Object>> thenConditionallyApply(
                FunctionWithException<T, Boolean, Exception> condition,
                FunctionWithException<T, U, Exception> actionIfTrue,
                FunctionWithException<T, V, Exception> actionIfFalse)
                throws DeclarationException {
            preCheck();
            DeclarationStage<Tuple2<Boolean, Object>> next = new DeclarationStage<>();
            ConditionalBiApplyTransformation<T, U, V> trans =
                    new ConditionalBiApplyTransformation<>(
                            condition, actionIfTrue, actionIfFalse, next);
            transformations.add(trans);
            currentStage = next;
            getLastTransformation().declare();
            return next;
        }

        public <U> DeclarationStage<Tuple2<Boolean, U>> thenConditionallyApply(
                FunctionWithException<T, Boolean, ? extends Exception> condition,
                FunctionWithException<T, U, ? extends Exception> actionIfTrue)
                throws DeclarationException {
            preCheck();
            DeclarationStage<Tuple2<Boolean, U>> next = new DeclarationStage<>();
            ConditionalApplyTransformation<T, U> trans =
                    new ConditionalApplyTransformation<>(condition, actionIfTrue, next);
            transformations.add(trans);
            currentStage = next;
            getLastTransformation().declare();
            return next;
        }

        public DeclarationStage<Boolean> thenConditionallyAccept(
                FunctionWithException<T, Boolean, ? extends Exception> condition,
                ThrowingConsumer<T, ? extends Exception> actionIfTrue,
                ThrowingConsumer<T, ? extends Exception> actionIfFalse)
                throws DeclarationException {
            preCheck();
            DeclarationStage<Boolean> next = new DeclarationStage<>();
            ConditionalBiAcceptTransformation<T> trans =
                    new ConditionalBiAcceptTransformation<>(
                            condition, actionIfTrue, actionIfFalse, next);
            transformations.add(trans);
            currentStage = next;
            getLastTransformation().declare();
            return next;
        }

        public DeclarationStage<Boolean> thenConditionallyAccept(
                FunctionWithException<T, Boolean, ? extends Exception> condition,
                ThrowingConsumer<T, ? extends Exception> actionIfTrue)
                throws DeclarationException {
            preCheck();
            DeclarationStage<Boolean> next = new DeclarationStage<>();
            ConditionalAcceptTransformation<T> trans =
                    new ConditionalAcceptTransformation<>(condition, actionIfTrue, next);
            transformations.add(trans);
            currentStage = next;
            getLastTransformation().declare();
            return next;
        }

        public <U, V> DeclarationStage<Tuple2<Boolean, Object>> thenConditionallyCompose(
                FunctionWithException<T, Boolean, ? extends Exception> condition,
                FunctionWithException<T, StateFuture<U>, ? extends Exception> actionIfTrue,
                FunctionWithException<T, StateFuture<V>, ? extends Exception> actionIfFalse)
                throws DeclarationException {
            preCheck();
            DeclarationStage<Tuple2<Boolean, Object>> next = new DeclarationStage<>();
            ConditionalBiComposeTransformation<T, U, V> trans =
                    new ConditionalBiComposeTransformation<>(
                            condition, actionIfTrue, actionIfFalse, next);
            transformations.add(trans);
            currentStage = next;
            getLastTransformation().declare();
            return next;
        }

        public <U> DeclarationStage<Tuple2<Boolean, U>> thenConditionallyCompose(
                FunctionWithException<T, Boolean, ? extends Exception> condition,
                FunctionWithException<T, StateFuture<U>, ? extends Exception> actionIfTrue)
                throws DeclarationException {
            preCheck();
            DeclarationStage<Tuple2<Boolean, U>> next = new DeclarationStage<>();
            ConditionalComposeTransformation<T, U> trans =
                    new ConditionalComposeTransformation<>(condition, actionIfTrue, next);
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
                FunctionWithException<FROM, StateFuture<TO>, ? extends Exception> action,
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

        ThrowingConsumer<FROM, ? extends Exception> action;

        NamedConsumer<FROM> namedFunction;

        AcceptTransformation(
                ThrowingConsumer<FROM, ? extends Exception> action, DeclarationStage<Void> to) {
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

    class ApplyTransformation<FROM, TO> extends AbstractTransformation<FROM, TO> {

        DeclarationStage<TO> to;

        FunctionWithException<FROM, TO, ? extends Exception> action;

        NamedFunction<FROM, TO> namedFunction;

        ApplyTransformation(
                FunctionWithException<FROM, TO, ? extends Exception> action,
                DeclarationStage<TO> to) {
            this.action = action;
            this.to = to;
        }

        @Override
        public StateFuture<TO> apply(StateFuture<FROM> upstream) throws Exception {
            return upstream.thenApply(namedFunction);
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

    static final String CONDITION = "%_COND_%";
    static final String TRUE_ACTION = "%_AC_T_%";
    static final String FALSE_ACTION = "%_AC_F_%";

    class ConditionalBiApplyTransformation<FROM, U, V>
            extends AbstractTransformation<FROM, Tuple2<Boolean, Object>> {

        DeclarationStage<Tuple2<Boolean, Object>> to;

        FunctionWithException<FROM, Boolean, ? extends Exception> condition;
        FunctionWithException<FROM, U, ? extends Exception> actionIfTrue;
        FunctionWithException<FROM, V, ? extends Exception> actionIfFalse;

        NamedFunction<FROM, Boolean> namedCondition;
        NamedFunction<FROM, U> namedActionIfTrue;
        NamedFunction<FROM, V> namedActionIfFalse;

        ConditionalBiApplyTransformation(
                FunctionWithException<FROM, Boolean, ? extends Exception> condition,
                FunctionWithException<FROM, U, ? extends Exception> actionIfTrue,
                FunctionWithException<FROM, V, ? extends Exception> actionIfFalse,
                DeclarationStage<Tuple2<Boolean, Object>> to) {
            this.condition = condition;
            this.actionIfTrue = actionIfTrue;
            this.actionIfFalse = actionIfFalse;
            this.to = to;
        }

        @Override
        public StateFuture<Tuple2<Boolean, Object>> apply(StateFuture<FROM> upstream)
                throws Exception {
            return upstream.thenConditionallyApply(
                    namedCondition, namedActionIfTrue, namedActionIfFalse);
        }

        @Override
        public void declare() throws DeclarationException {
            if (namedCondition == null) {
                if (name == null) {
                    namedCondition = context.declare(condition);
                    namedActionIfTrue = context.declare(actionIfTrue);
                    namedActionIfFalse = context.declare(actionIfFalse);
                } else {
                    namedCondition = context.declare(name + CONDITION, condition);
                    namedActionIfTrue = context.declare(name + TRUE_ACTION, actionIfTrue);
                    namedActionIfFalse = context.declare(name + FALSE_ACTION, actionIfFalse);
                }
            }
        }
    }

    class ConditionalApplyTransformation<FROM, U>
            extends AbstractTransformation<FROM, Tuple2<Boolean, U>> {

        DeclarationStage<Tuple2<Boolean, U>> to;

        FunctionWithException<FROM, Boolean, ? extends Exception> condition;
        FunctionWithException<FROM, U, ? extends Exception> actionIfTrue;

        NamedFunction<FROM, Boolean> namedCondition;
        NamedFunction<FROM, U> namedActionIfTrue;

        ConditionalApplyTransformation(
                FunctionWithException<FROM, Boolean, ? extends Exception> condition,
                FunctionWithException<FROM, U, ? extends Exception> actionIfTrue,
                DeclarationStage<Tuple2<Boolean, U>> to) {
            this.condition = condition;
            this.actionIfTrue = actionIfTrue;
            this.to = to;
        }

        @Override
        public StateFuture<Tuple2<Boolean, U>> apply(StateFuture<FROM> upstream) throws Exception {
            return upstream.thenConditionallyApply(namedCondition, namedActionIfTrue);
        }

        @Override
        public void declare() throws DeclarationException {
            if (namedCondition == null) {
                if (name == null) {
                    namedCondition = context.declare(condition);
                    namedActionIfTrue = context.declare(actionIfTrue);
                } else {
                    namedCondition = context.declare(name + CONDITION, condition);
                    namedActionIfTrue = context.declare(name + TRUE_ACTION, actionIfTrue);
                }
            }
        }
    }

    class ConditionalBiAcceptTransformation<FROM> extends AbstractTransformation<FROM, Boolean> {

        DeclarationStage<Boolean> to;

        FunctionWithException<FROM, Boolean, ? extends Exception> condition;
        ThrowingConsumer<FROM, ? extends Exception> actionIfTrue;
        ThrowingConsumer<FROM, ? extends Exception> actionIfFalse;

        NamedFunction<FROM, Boolean> namedCondition;
        NamedConsumer<FROM> namedActionIfTrue;
        NamedConsumer<FROM> namedActionIfFalse;

        ConditionalBiAcceptTransformation(
                FunctionWithException<FROM, Boolean, ? extends Exception> condition,
                ThrowingConsumer<FROM, ? extends Exception> actionIfTrue,
                ThrowingConsumer<FROM, ? extends Exception> actionIfFalse,
                DeclarationStage<Boolean> to) {
            this.condition = condition;
            this.actionIfTrue = actionIfTrue;
            this.actionIfFalse = actionIfFalse;
            this.to = to;
        }

        @Override
        public StateFuture<Boolean> apply(StateFuture<FROM> upstream) throws Exception {
            return upstream.thenConditionallyAccept(
                    namedCondition, namedActionIfTrue, namedActionIfFalse);
        }

        @Override
        public void declare() throws DeclarationException {
            if (namedCondition == null) {
                if (name == null) {
                    namedCondition = context.declare(condition);
                    namedActionIfTrue = context.declare(actionIfTrue);
                    namedActionIfFalse = context.declare(actionIfFalse);
                } else {
                    namedCondition = context.declare(name + CONDITION, condition);
                    namedActionIfTrue = context.declare(name + TRUE_ACTION, actionIfTrue);
                    namedActionIfFalse = context.declare(name + FALSE_ACTION, actionIfFalse);
                }
            }
        }
    }

    class ConditionalAcceptTransformation<FROM> extends AbstractTransformation<FROM, Boolean> {

        DeclarationStage<Boolean> to;

        FunctionWithException<FROM, Boolean, ? extends Exception> condition;
        ThrowingConsumer<FROM, ? extends Exception> actionIfTrue;

        NamedFunction<FROM, Boolean> namedCondition;
        NamedConsumer<FROM> namedActionIfTrue;

        ConditionalAcceptTransformation(
                FunctionWithException<FROM, Boolean, ? extends Exception> condition,
                ThrowingConsumer<FROM, ? extends Exception> actionIfTrue,
                DeclarationStage<Boolean> to) {
            this.condition = condition;
            this.actionIfTrue = actionIfTrue;
            this.to = to;
        }

        @Override
        public StateFuture<Boolean> apply(StateFuture<FROM> upstream) throws Exception {
            return upstream.thenConditionallyAccept(namedCondition, namedActionIfTrue);
        }

        @Override
        public void declare() throws DeclarationException {
            if (namedCondition == null) {
                if (name == null) {
                    namedCondition = context.declare(condition);
                    namedActionIfTrue = context.declare(actionIfTrue);
                } else {
                    namedCondition = context.declare(name + CONDITION, condition);
                    namedActionIfTrue = context.declare(name + TRUE_ACTION, actionIfTrue);
                }
            }
        }
    }

    class ConditionalBiComposeTransformation<FROM, U, V>
            extends AbstractTransformation<FROM, Tuple2<Boolean, Object>> {

        DeclarationStage<Tuple2<Boolean, Object>> to;

        FunctionWithException<FROM, Boolean, ? extends Exception> condition;
        FunctionWithException<FROM, StateFuture<U>, ? extends Exception> actionIfTrue;
        FunctionWithException<FROM, StateFuture<V>, ? extends Exception> actionIfFalse;

        NamedFunction<FROM, Boolean> namedCondition;
        NamedFunction<FROM, StateFuture<U>> namedActionIfTrue;
        NamedFunction<FROM, StateFuture<V>> namedActionIfFalse;

        ConditionalBiComposeTransformation(
                FunctionWithException<FROM, Boolean, ? extends Exception> condition,
                FunctionWithException<FROM, StateFuture<U>, ? extends Exception> actionIfTrue,
                FunctionWithException<FROM, StateFuture<V>, ? extends Exception> actionIfFalse,
                DeclarationStage<Tuple2<Boolean, Object>> to) {
            this.condition = condition;
            this.actionIfTrue = actionIfTrue;
            this.actionIfFalse = actionIfFalse;
            this.to = to;
        }

        @Override
        public StateFuture<Tuple2<Boolean, Object>> apply(StateFuture<FROM> upstream)
                throws Exception {
            return upstream.thenConditionallyCompose(
                    namedCondition, namedActionIfTrue, namedActionIfFalse);
        }

        @Override
        public void declare() throws DeclarationException {
            if (namedCondition == null) {
                if (name == null) {
                    namedCondition = context.declare(condition);
                    namedActionIfTrue = context.declare(actionIfTrue);
                    namedActionIfFalse = context.declare(actionIfFalse);
                } else {
                    namedCondition = context.declare(name + CONDITION, condition);
                    namedActionIfTrue = context.declare(name + TRUE_ACTION, actionIfTrue);
                    namedActionIfFalse = context.declare(name + FALSE_ACTION, actionIfFalse);
                }
            }
        }
    }

    class ConditionalComposeTransformation<FROM, U>
            extends AbstractTransformation<FROM, Tuple2<Boolean, U>> {

        DeclarationStage<Tuple2<Boolean, U>> to;

        FunctionWithException<FROM, Boolean, ? extends Exception> condition;
        FunctionWithException<FROM, StateFuture<U>, ? extends Exception> actionIfTrue;

        NamedFunction<FROM, Boolean> namedCondition;
        NamedFunction<FROM, StateFuture<U>> namedActionIfTrue;

        ConditionalComposeTransformation(
                FunctionWithException<FROM, Boolean, ? extends Exception> condition,
                FunctionWithException<FROM, StateFuture<U>, ? extends Exception> actionIfTrue,
                DeclarationStage<Tuple2<Boolean, U>> to) {
            this.condition = condition;
            this.actionIfTrue = actionIfTrue;
            this.to = to;
        }

        @Override
        public StateFuture<Tuple2<Boolean, U>> apply(StateFuture<FROM> upstream) throws Exception {
            return upstream.thenConditionallyCompose(namedCondition, namedActionIfTrue);
        }

        @Override
        public void declare() throws DeclarationException {
            if (namedCondition == null) {
                if (name == null) {
                    namedCondition = context.declare(condition);
                    namedActionIfTrue = context.declare(actionIfTrue);
                } else {
                    namedCondition = context.declare(name + CONDITION, condition);
                    namedActionIfTrue = context.declare(name + TRUE_ACTION, actionIfTrue);
                }
            }
        }
    }
}
