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

package org.apache.flink.streaming.examples.allowlatency.utils;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * A stream sink that counts the number of all elements. The counting result is stored in an {@link
 * org.apache.flink.api.common.accumulators.Accumulator} specified by {@link #COUNTER_NAME} and can
 * be acquired by {@link
 * org.apache.flink.api.common.JobExecutionResult#getAccumulatorResult(String)}.
 *
 * @param <T> The type of elements received by the sink.
 */
public class CountingAndDiscardingSink<T> extends RichSinkFunction<T> {
    public static final String COUNTER_NAME = "numElements";

    private static final long serialVersionUID = 1L;

    private final LongCounter numElementsCounter = new LongCounter();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(COUNTER_NAME, numElementsCounter);
    }

    @Override
    public void invoke(T value, Context context) {
        numElementsCounter.add(1L);
    }
}
