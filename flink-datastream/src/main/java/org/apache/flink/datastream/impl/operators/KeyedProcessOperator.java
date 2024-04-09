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
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.impl.common.KeyCheckedOutputCollector;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;

import javax.annotation.Nullable;

/** Operator for {@link OneInputStreamProcessFunction} in {@link KeyedPartitionStream}. */
public class KeyedProcessOperator<KEY, IN, OUT> extends ProcessOperator<IN, OUT> {

    @Nullable private final KeySelector<OUT, KEY> outKeySelector;

    public KeyedProcessOperator(OneInputStreamProcessFunction<IN, OUT> userFunction) {
        this(userFunction, null);
    }

    public KeyedProcessOperator(
            OneInputStreamProcessFunction<IN, OUT> userFunction,
            @Nullable KeySelector<OUT, KEY> outKeySelector) {
        super(userFunction);
        this.outKeySelector = outKeySelector;
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
}
