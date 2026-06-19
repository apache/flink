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
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.impl.common.KeyCheckedOutputCollector;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultTwoOutputNonPartitionedContext;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;

/** Base operator for {@link TwoOutputStreamProcessFunction} in {@link KeyedPartitionStream}. */
public class BaseKeyedTwoOutputProcessOperator<KEY, IN, OUT_MAIN, OUT_SIDE>
        extends TwoOutputProcessOperator<IN, OUT_MAIN, OUT_SIDE> {

    // TODO Restore this keySet when task initialized from checkpoint.
    protected transient Set<Object> keySet;

    @Nullable protected final KeySelector<OUT_MAIN, KEY> mainOutKeySelector;

    @Nullable protected final KeySelector<OUT_SIDE, KEY> sideOutKeySelector;

    public BaseKeyedTwoOutputProcessOperator(
            TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE> userFunction,
            OutputTag<OUT_SIDE> outputTag,
            @Nullable KeySelector<OUT_MAIN, KEY> mainOutKeySelector,
            @Nullable KeySelector<OUT_SIDE, KEY> sideOutKeySelector) {
        super(userFunction, outputTag);
        Preconditions.checkArgument(
                (mainOutKeySelector == null && sideOutKeySelector == null)
                        || (mainOutKeySelector != null && sideOutKeySelector != null),
                "Both mainOutKeySelector and sideOutKeySelector must be null or not null.");
        this.mainOutKeySelector = mainOutKeySelector;
        this.sideOutKeySelector = sideOutKeySelector;
    }

    @Override
    public void open() throws Exception {
        this.keySet = new HashSet<>();
        super.open();
    }

    @Override
    protected TimestampCollector<OUT_MAIN> getMainCollector() {
        return mainOutKeySelector != null && sideOutKeySelector != null
                ? new KeyCheckedOutputCollector<>(
                        new OutputCollector<>(output),
                        mainOutKeySelector,
                        () -> (KEY) getCurrentKey())
                : new OutputCollector<>(output);
    }

    @Override
    public TimestampCollector<OUT_SIDE> getSideCollector() {
        return mainOutKeySelector != null && sideOutKeySelector != null
                ? new KeyCheckedOutputCollector<>(
                        new SideOutputCollector(output),
                        sideOutKeySelector,
                        () -> (KEY) getCurrentKey())
                : new SideOutputCollector(output);
    }

    @Override
    protected Object currentKey() {
        return getCurrentKey();
    }

    @Override
    protected TwoOutputNonPartitionedContext<OUT_MAIN, OUT_SIDE> getNonPartitionedContext() {
        return new DefaultTwoOutputNonPartitionedContext<>(
                context,
                partitionedContext,
                mainCollector,
                sideCollector,
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
    public boolean isAsyncKeyOrderedProcessingEnabled() {
        return true;
    }
}
