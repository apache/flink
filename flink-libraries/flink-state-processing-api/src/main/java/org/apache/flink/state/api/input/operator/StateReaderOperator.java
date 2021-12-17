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

package org.apache.flink.state.api.input.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.state.api.runtime.SavepointRuntimeContext;
import org.apache.flink.state.api.runtime.VoidTriggerable;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Base class for executing functions that read keyed state.
 *
 * @param <F> The type of the user function.
 * @param <KEY> The key type.
 * @param <N> The namespace type.
 * @param <OUT> The output type.
 */
@Internal
public abstract class StateReaderOperator<F extends Function, KEY, N, OUT>
        implements KeyContext, Serializable {

    private static final long serialVersionUID = 1L;

    protected final F function;

    private final TypeInformation<KEY> keyType;

    protected final TypeSerializer<N> namespaceSerializer;

    private transient ExecutionConfig executionConfig;

    private transient KeyedStateBackend<KEY> keyedStateBackend;

    private transient TypeSerializer<KEY> keySerializer;

    private transient InternalTimeServiceManager<KEY> timerServiceManager;

    protected StateReaderOperator(
            F function, TypeInformation<KEY> keyType, TypeSerializer<N> namespaceSerializer) {
        Preconditions.checkNotNull(function, "The user function must not be null");
        Preconditions.checkNotNull(keyType, "The key type must not be null");
        Preconditions.checkNotNull(
                namespaceSerializer, "The namespace serializer must not be null");

        this.function = function;
        this.keyType = keyType;
        this.namespaceSerializer = namespaceSerializer;
    }

    public abstract void processElement(KEY key, N namespace, Collector<OUT> out) throws Exception;

    public abstract CloseableIterator<Tuple2<KEY, N>> getKeysAndNamespaces(
            SavepointRuntimeContext ctx) throws Exception;

    public final void setup(
            ExecutionConfig executionConfig,
            KeyedStateBackend<KEY> keyKeyedStateBackend,
            InternalTimeServiceManager<KEY> timerServiceManager,
            SavepointRuntimeContext ctx) {

        this.executionConfig = executionConfig;
        this.keyedStateBackend = keyKeyedStateBackend;
        this.timerServiceManager = timerServiceManager;
        this.keySerializer = keyType.createSerializer(executionConfig);

        FunctionUtils.setFunctionRuntimeContext(function, ctx);
    }

    protected final InternalTimerService<N> getInternalTimerService(String name) {
        return timerServiceManager.getInternalTimerService(
                name, keySerializer, namespaceSerializer, VoidTriggerable.instance());
    }

    public void open() throws Exception {
        FunctionUtils.openFunction(function, new Configuration());
    }

    public void close() throws Exception {
        Exception exception = null;

        try {
            FunctionUtils.closeFunction(function);
        } catch (Exception e) {
            // The state backend must always be closed
            // to release native resources.
            exception = e;
        }

        keyedStateBackend.dispose();

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public final void setCurrentKey(Object key) {
        keyedStateBackend.setCurrentKey((KEY) key);
    }

    @Override
    public final Object getCurrentKey() {
        return keyedStateBackend.getCurrentKey();
    }

    public final KeyedStateBackend<KEY> getKeyedStateBackend() {
        return keyedStateBackend;
    }

    public final TypeInformation<KEY> getKeyType() {
        return keyType;
    }

    public final ExecutionConfig getExecutionConfig() {
        return this.executionConfig;
    }
}
