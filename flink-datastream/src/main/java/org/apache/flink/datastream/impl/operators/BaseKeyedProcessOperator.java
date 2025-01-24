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

package org.apache.flink.datastream.impl.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.impl.common.KeyCheckedOutputCollector;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultNonPartitionedContext;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;

/** Base operator for {@link OneInputStreamProcessFunction} in {@link KeyedPartitionStream}. */
public class BaseKeyedProcessOperator<KEY, IN, OUT> extends ProcessOperator<IN, OUT> {
    // TODO Restore this keySet when task initialized from checkpoint.
    protected transient Set<Object> keySet;

    @Nullable protected final KeySelector<OUT, KEY> outKeySelector;

    public BaseKeyedProcessOperator(OneInputStreamProcessFunction<IN, OUT> userFunction) {
        this(userFunction, null);
    }

    public BaseKeyedProcessOperator(
            OneInputStreamProcessFunction<IN, OUT> userFunction,
            @Nullable KeySelector<OUT, KEY> outKeySelector) {
        super(userFunction);
        this.outKeySelector = outKeySelector;
    }

    @Override
    public void open() throws Exception {
        this.keySet = new HashSet<>();
        super.open();
    }

    @Override
    protected TimestampCollector<OUT> getOutputCollector() {
        return outKeySelector != null
                ? new KeyCheckedOutputCollector<>(
                        new OutputCollector<>(output), outKeySelector, () -> (KEY) getCurrentKey())
                : new OutputCollector<>(output);
    }

    @Override
    protected Object currentKey() {
        return getCurrentKey();
    }

    @Override
    protected NonPartitionedContext<OUT> getNonPartitionedContext() {
        return new DefaultNonPartitionedContext<>(
                context,
                partitionedContext,
                outputCollector,
                true,
                keySet,
                output,
                watermarkDeclarationMap);
    }

    @Override
    public void newKeySelected(Object newKey) {
        keySet.add(newKey);
    }

    @Override
    public boolean isAsyncStateProcessingEnabled() {
        return true;
    }
}
