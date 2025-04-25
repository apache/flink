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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

/** The default implementation of {@link PartitionedContext}. */
public class DefaultPartitionedContext<OUT> extends AbstractPartitionedContext
        implements PartitionedContext<OUT> {

    /**
     * The {@link DefaultNonPartitionedContext} and {@link DefaultPartitionedContext} create a
     * circular reference, so the {@code nonPartitionedContext} field of {@link
     * DefaultPartitionedContext} should be set in a separate method, {@link
     * DefaultPartitionedContext#setNonPartitionedContext(NonPartitionedContext)}, rather than in
     * the constructor.
     */
    private NonPartitionedContext<OUT> nonPartitionedContext;

    public DefaultPartitionedContext(
            RuntimeContext context,
            Supplier<Object> currentKeySupplier,
            BiConsumer<Runnable, Object> processorWithKey,
            ProcessingTimeManager processingTimeManager,
            StreamingRuntimeContext operatorContext,
            OperatorStateStore operatorStateStore) {
        super(
                context,
                currentKeySupplier,
                processorWithKey,
                processingTimeManager,
                operatorContext,
                operatorStateStore);
    }

    public void setNonPartitionedContext(NonPartitionedContext<OUT> nonPartitionedContext) {
        this.nonPartitionedContext = nonPartitionedContext;
    }

    @Override
    public NonPartitionedContext<OUT> getNonPartitionedContext() {
        return nonPartitionedContext;
    }
}
