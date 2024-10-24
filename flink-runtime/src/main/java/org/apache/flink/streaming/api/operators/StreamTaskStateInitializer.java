/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This is the interface through which stream task expose a {@link StreamOperatorStateContext} to
 * their operators. Operators, in turn, can use the context to initialize everything connected to
 * their state, such as backends or a timer service manager.
 */
public interface StreamTaskStateInitializer {

    /**
     * Returns the {@link StreamOperatorStateContext} for an {@link AbstractStreamOperator} that
     * runs in the stream task that owns this manager.
     *
     * @param operatorID the id of the operator for which the context is created. Cannot be null.
     * @param operatorClassName the classname of the operator instance for which the context is
     *     created. Cannot be null.
     * @param processingTimeService
     * @param keyContext the key context of the operator instance for which the context is created
     *     Cannot be null.
     * @param keySerializer the key-serializer for the operator. Can be null.
     * @param streamTaskCloseableRegistry the closeable registry to which created closeable objects
     *     will be registered.
     * @param metricGroup the parent metric group for all statebackend metrics
     * @param managedMemoryFraction the managed memory fraction of the operator for state backend
     * @param isUsingCustomRawKeyedState flag indicating whether or not the {@link
     *     AbstractStreamOperator} is writing custom raw keyed state.
     * @return a context from which the given operator can initialize everything related to state.
     * @throws Exception when something went wrong while creating the context.
     */
    StreamOperatorStateContext streamOperatorStateContext(
            @Nonnull OperatorID operatorID,
            @Nonnull String operatorClassName,
            @Nonnull ProcessingTimeService processingTimeService,
            @Nonnull KeyContext keyContext,
            @Nullable TypeSerializer<?> keySerializer,
            @Nonnull CloseableRegistry streamTaskCloseableRegistry,
            @Nonnull MetricGroup metricGroup,
            double managedMemoryFraction,
            boolean isUsingCustomRawKeyedState,
            boolean isAsyncState)
            throws Exception;
}
