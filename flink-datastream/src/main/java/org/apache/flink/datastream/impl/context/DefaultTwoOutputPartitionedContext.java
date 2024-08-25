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
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.function.Consumer;
import java.util.function.Supplier;

/** The default implementation of {@link PartitionedContext}. */
public class DefaultTwoOutputPartitionedContext extends DefaultAbstractPartitionedContext
        implements TwoOutputPartitionedContext {

    protected final TwoOutputNonPartitionedContext<?, ?> nonPartitionedContext;

    public DefaultTwoOutputPartitionedContext(
            RuntimeContext context,
            Supplier<Object> currentKeySupplier,
            Consumer<Object> currentKeySetter,
            ProcessingTimeManager processingTimeManager,
            StreamingRuntimeContext operatorContext,
            OperatorStateStore operatorStateStore,
            TwoOutputNonPartitionedContext<?, ?> nonPartitionedContext) {
        super(
                context,
                currentKeySupplier,
                currentKeySetter,
                processingTimeManager,
                operatorContext,
                operatorStateStore);
        this.nonPartitionedContext = nonPartitionedContext;
    }

    @Override
    public TwoOutputNonPartitionedContext<?, ?> getNonPartitionedContext() {
        return nonPartitionedContext;
    }
}
