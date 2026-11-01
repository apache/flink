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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.util.ratelimit.DynamicRateLimitParams;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.util.Preconditions;

/**
 * The DataStreamSource represents the starting point of a DataStream.
 *
 * @param <T> Type of the elements in the DataStream created from the this source.
 */
@Public
public class DataStreamSource<T> extends SingleOutputStreamOperator<T> {

    private boolean isParallel;

    public DataStreamSource(
            StreamExecutionEnvironment environment,
            TypeInformation<T> outTypeInfo,
            StreamSource<T, ?> operator,
            boolean isParallel,
            String sourceName) {
        this(
                environment,
                outTypeInfo,
                operator,
                isParallel,
                sourceName,
                Boundedness.CONTINUOUS_UNBOUNDED);
    }

    /** The constructor used to create legacy sources. */
    public DataStreamSource(
            StreamExecutionEnvironment environment,
            TypeInformation<T> outTypeInfo,
            StreamSource<T, ?> operator,
            boolean isParallel,
            String sourceName,
            Boundedness boundedness) {
        super(
                environment,
                createSourceTransformation(
                        environment, outTypeInfo, operator, isParallel, sourceName, boundedness));

        this.isParallel = isParallel;
        if (!isParallel) {
            setParallelism(1);
        }
    }

    private static <T> LegacySourceTransformation<T> createSourceTransformation(
            StreamExecutionEnvironment environment,
            TypeInformation<T> outTypeInfo,
            StreamSource<T, ?> operator,
            boolean isParallel,
            String sourceName,
            Boundedness boundedness) {
        LegacySourceTransformation<T> transformation =
                new LegacySourceTransformation<>(
                        sourceName,
                        operator,
                        outTypeInfo,
                        environment.getParallelism(),
                        boundedness,
                        false);
        transformation.setChainingStrategy(ChainingStrategy.HEAD);
        return transformation;
    }

    /**
     * Constructor for "deep" sources that manually set up (one or more) custom configured complex
     * operators.
     */
    public DataStreamSource(SingleOutputStreamOperator<T> operator) {
        super(operator.environment, operator.getTransformation());
        this.isParallel = true;
    }

    /** Constructor for new Sources (FLIP-27). */
    public DataStreamSource(
            StreamExecutionEnvironment environment,
            Source<T, ?, ?> source,
            WatermarkStrategy<T> watermarkStrategy,
            TypeInformation<T> outTypeInfo,
            String sourceName) {
        super(
                environment,
                new SourceTransformation<>(
                        sourceName,
                        source,
                        watermarkStrategy,
                        outTypeInfo,
                        environment.getParallelism(),
                        false));
        this.isParallel = true;
    }

    @VisibleForTesting
    boolean isParallel() {
        return isParallel;
    }

    /**
     * Overrides the job-wide {@link org.apache.flink.configuration.SourceOptions} for this source
     * only.
     *
     * <p>This only takes effect for the connector if its {@code SourceReader} was built with a
     * {@link org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy#dynamic
     * dynamic} {@code RateLimiterStrategy}; otherwise this call has no observable effect.
     *
     * @throws IllegalStateException if this {@code DataStreamSource} does not wrap a FLIP-27 {@link
     *     Source} (e.g. it was created from a legacy {@code SourceFunction}).
     */
    public DataStreamSource<T> withDynamicRateLimitParams(
            DynamicRateLimitParams dynamicRateLimitParams) {
        Preconditions.checkNotNull(dynamicRateLimitParams);
        Transformation<T> transformation = getTransformation();
        Preconditions.checkState(
                transformation instanceof SourceTransformation,
                "withDynamicRateLimitParams() is only supported for FLIP-27 sources created "
                        + "via StreamExecutionEnvironment#fromSource(...).");
        ((SourceTransformation<T, ?, ?>) transformation)
                .setDynamicRateLimitParams(dynamicRateLimitParams);
        return this;
    }

    @Override
    public DataStreamSource<T> setParallelism(int parallelism) {
        OperatorValidationUtils.validateParallelism(parallelism, isParallel);
        super.setParallelism(parallelism);
        return this;
    }
}
