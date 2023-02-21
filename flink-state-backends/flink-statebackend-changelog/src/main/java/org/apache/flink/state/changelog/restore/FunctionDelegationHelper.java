/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog.restore;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link DelegatingFunction Delegating functions} are used to create metadata on recovery when the
 * actual function code is not known yet. Once the actual function is known, backend updates the
 * delegate which starts receiving the calls.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
@Internal
public class FunctionDelegationHelper {
    private static final Logger LOG = LoggerFactory.getLogger(FunctionDelegationHelper.class);

    public static <T> ReduceFunction<T> delegateReduceFunction() {
        return new DelegatingReduceFunction<>();
    }

    public static <IN, ACC, OUT> AggregateFunction<IN, ACC, OUT> delegateAggregateFunction() {
        return new DelegatingAggregateFunction<>();
    }

    private interface DelegatingFunction<F> extends Function {
        void delegateIfNeeded(F delegated);
    }

    private final Map<String, DelegatingFunction> delegatingFunctions = new HashMap<>();

    public <T, S extends State, F> void addOrUpdate(StateDescriptor<S, T> stateDescriptor) {
        F function = tryGetFunction(stateDescriptor);
        String name = stateDescriptor.getName();
        if (function instanceof DelegatingFunction) {
            LOG.debug("add delegate: {}", name);
            delegatingFunctions.putIfAbsent(name, (DelegatingFunction<?>) function);
        } else {
            DelegatingFunction<F> delegating = delegatingFunctions.get(name);
            if (delegating != null) {
                LOG.debug("update delegate: {}", name);
                checkState(function != null, "unable to extract function for state " + name);
                delegating.delegateIfNeeded(function);
            }
        }
    }

    @Nullable
    private static <F extends Function> F tryGetFunction(StateDescriptor<?, ?> stateDescriptor) {
        if (stateDescriptor instanceof ReducingStateDescriptor) {
            return (F) ((ReducingStateDescriptor) stateDescriptor).getReduceFunction();
        } else if (stateDescriptor instanceof AggregatingStateDescriptor) {
            return (F) ((AggregatingStateDescriptor) stateDescriptor).getAggregateFunction();
        } else {
            return null;
        }
    }

    static class DelegatingAggregateFunction<IN, ACC, OUT>
            implements AggregateFunction<IN, ACC, OUT>,
                    DelegatingFunction<AggregateFunction<IN, ACC, OUT>> {
        private static final long serialVersionUID = 1L;

        @Nullable private AggregateFunction<IN, ACC, OUT> delegated;

        @Override
        public void delegateIfNeeded(AggregateFunction<IN, ACC, OUT> delegated) {
            if (this.delegated == null) {
                this.delegated = checkNotNull(delegated);
            }
        }

        @Override
        public ACC createAccumulator() {
            checkNotNull(delegated);
            return delegated.createAccumulator();
        }

        @Override
        public ACC add(IN value, ACC accumulator) {
            checkNotNull(delegated);
            return delegated.add(value, accumulator);
        }

        @Override
        public OUT getResult(ACC accumulator) {
            checkNotNull(delegated);
            return delegated.getResult(accumulator);
        }

        @Override
        public ACC merge(ACC a, ACC b) {
            checkNotNull(delegated);
            return delegated.merge(a, b);
        }
    }

    static class DelegatingReduceFunction<T>
            implements ReduceFunction<T>, DelegatingFunction<ReduceFunction<T>> {
        private static final long serialVersionUID = 1L;

        @Nullable private ReduceFunction<T> delegated;

        @Override
        public T reduce(T left, T right) throws Exception {
            checkNotNull(delegated);
            return delegated.reduce(left, right);
        }

        @Override
        public void delegateIfNeeded(ReduceFunction<T> delegated) {
            if (this.delegated == null) {
                this.delegated = checkNotNull(delegated);
            }
        }
    }
}
