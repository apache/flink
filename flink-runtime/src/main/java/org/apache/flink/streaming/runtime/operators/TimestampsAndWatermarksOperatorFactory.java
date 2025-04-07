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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * TimestampsAndWatermarksOperator}.
 */
public class TimestampsAndWatermarksOperatorFactory<T> extends AbstractStreamOperatorFactory<T>
        implements OneInputStreamOperatorFactory<T, T> {

    private final WatermarkStrategy<T> watermarkStrategy;

    /** Whether to emit intermediate watermarks or only one final watermark at the end of input. */
    private final boolean emitProgressiveWatermarks;

    public TimestampsAndWatermarksOperatorFactory(
            WatermarkStrategy<T> watermarkStrategy, boolean emitProgressiveWatermarks) {
        this.watermarkStrategy = checkNotNull(watermarkStrategy);
        this.emitProgressiveWatermarks = emitProgressiveWatermarks;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T1 extends StreamOperator<T>> T1 createStreamOperator(
            StreamOperatorParameters<T> parameters) {
        return (T1)
                new TimestampsAndWatermarksOperator<>(
                        parameters, watermarkStrategy, emitProgressiveWatermarks);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return TimestampsAndWatermarksOperator.class;
    }
}
