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

package org.apache.flink.connector.datagen.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceReaderFactory;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A data source that produces N data points in parallel. This source is useful for testing and for
 * cases that just need a stream of N events of any kind.
 *
 * <p>The source splits the sequence into as many parallel sub-sequences as there are parallel
 * source readers.
 *
 * <p>Users can supply a {@code GeneratorFunction} for mapping the (sub-)sequences of Long values
 * into the generated events. For instance, the following code will produce the sequence of
 * ["Number: 0", "Number: 1", ... , "Number: 999"] elements.
 *
 * <pre>{@code
 * GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;
 *
 * DataGeneratorSource<String> source =
 *         new DataGeneratorSource<>(generatorFunction, 1000, Types.STRING);
 *
 * DataStreamSource<String> stream =
 *         env.fromSource(source,
 *         WatermarkStrategy.noWatermarks(),
 *         "Generator Source");
 * }</pre>
 *
 * <p>The order of elements depends on the parallelism. Each sub-sequence will be produced in order.
 * Consequently, if the parallelism is limited to one, this will produce one sequence in order from
 * "Number: 0" to "Number: 999".
 *
 * <p>Note that this approach also makes it possible to produce deterministic watermarks at the
 * source based on the generated events and a custom {@code WatermarkStrategy}.
 *
 * <p>This source has built-in support for rate limiting. The following code will produce an
 * effectively unbounded (Long.MAX_VALUE from practical perspective will never be reached) stream of
 * Long values at the overall source rate (across all source subtasks) of 100 events per second.
 *
 * <pre>{@code
 * GeneratorFunction<Long, Long> generatorFunction = index -> index;
 *
 * DataGeneratorSource<String> source =
 *         new DataGeneratorSource<>(
 *              generatorFunctionStateless,
 *              Long.MAX_VALUE,
 *              RateLimiterStrategy.perSecond(100),
 *              Types.STRING);
 * }</pre>
 *
 * <p>This source is always bounded. For very long sequences (for example when the {@code count} is
 * set to Long.MAX_VALUE), users may want to consider executing the application in a streaming
 * manner, because, despite the fact that the produced stream is bounded, the end bound is pretty
 * far away.
 */
@Experimental
public class DataGeneratorSource<OUT>
        implements Source<OUT, NumberSequenceSplit, Collection<NumberSequenceSplit>>,
                ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private final SourceReaderFactory<OUT, NumberSequenceSplit> sourceReaderFactory;
    private final TypeInformation<OUT> typeInfo;

    private final NumberSequenceSource numberSource;

    /**
     * Instantiates a new {@code DataGeneratorSource}.
     *
     * @param generatorFunction The {@code GeneratorFunction} function.
     * @param count The number of generated data points.
     * @param typeInfo The type of the produced data points.
     */
    public DataGeneratorSource(
            GeneratorFunction<Long, OUT> generatorFunction,
            long count,
            TypeInformation<OUT> typeInfo) {
        this(generatorFunction, count, RateLimiterStrategy.noOp(), typeInfo);
    }

    /**
     * Instantiates a new {@code DataGeneratorSource}.
     *
     * @param generatorFunction The {@code GeneratorFunction} function.
     * @param count The number of generated data points.
     * @param rateLimiterStrategy The strategy for rate limiting.
     * @param typeInfo The type of the produced data points.
     */
    public DataGeneratorSource(
            GeneratorFunction<Long, OUT> generatorFunction,
            long count,
            RateLimiterStrategy rateLimiterStrategy,
            TypeInformation<OUT> typeInfo) {
        this(
                new GeneratorSourceReaderFactory<>(generatorFunction, rateLimiterStrategy),
                count,
                typeInfo);
        ClosureCleaner.clean(
                generatorFunction, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        ClosureCleaner.clean(
                rateLimiterStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
    }

    private DataGeneratorSource(
            SourceReaderFactory<OUT, NumberSequenceSplit> sourceReaderFactory,
            long count,
            TypeInformation<OUT> typeInfo) {
        this.sourceReaderFactory = checkNotNull(sourceReaderFactory);
        ClosureCleaner.clean(
                sourceReaderFactory, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        this.typeInfo = checkNotNull(typeInfo);
        this.numberSource = new NumberSequenceSource(0, count - 1);
    }

    // ------------------------------------------------------------------------
    //  source methods
    // ------------------------------------------------------------------------

    @Override
    public TypeInformation<OUT> getProducedType() {
        return typeInfo;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<OUT, NumberSequenceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return sourceReaderFactory.createReader(readerContext);
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> restoreEnumerator(
            SplitEnumeratorContext<NumberSequenceSplit> enumContext,
            Collection<NumberSequenceSplit> checkpoint) {
        return numberSource.restoreEnumerator(enumContext, checkpoint);
    }

    @Override
    public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> createEnumerator(
            final SplitEnumeratorContext<NumberSequenceSplit> enumContext) {
        return numberSource.createEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<NumberSequenceSplit> getSplitSerializer() {
        return numberSource.getSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<NumberSequenceSplit>>
            getEnumeratorCheckpointSerializer() {
        return numberSource.getEnumeratorCheckpointSerializer();
    }
}
