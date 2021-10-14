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

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Hybrid source that switches underlying sources based on configured source chain.
 *
 * <p>A simple example with FileSource and KafkaSource with fixed Kafka start position:
 *
 * <pre>{@code
 * FileSource<String> fileSource =
 *   FileSource.forRecordStreamFormat(new TextLineFormat(), Path.fromLocalFile(testDir)).build();
 * KafkaSource<String> kafkaSource =
 *           KafkaSource.<String>builder()
 *                   .setBootstrapServers("localhost:9092")
 *                   .setGroupId("MyGroup")
 *                   .setTopics(Arrays.asList("quickstart-events"))
 *                   .setDeserializer(
 *                           KafkaRecordDeserializer.valueOnly(StringDeserializer.class))
 *                   .setStartingOffsets(OffsetsInitializer.earliest())
 *                   .build();
 * HybridSource<String> hybridSource =
 *           HybridSource.builder(fileSource)
 *                   .addSource(kafkaSource)
 *                   .build();
 * }</pre>
 *
 * <p>A more complex example with Kafka start position derived from previous source:
 *
 * <pre>{@code
 * HybridSource<String> hybridSource =
 *     HybridSource.<String, StaticFileSplitEnumerator>builder(fileSource)
 *         .addSource(
 *             switchContext -> {
 *               StaticFileSplitEnumerator previousEnumerator =
 *                   switchContext.getPreviousEnumerator();
 *               // how to get timestamp depends on specific enumerator
 *               long timestamp = previousEnumerator.getEndTimestamp();
 *               OffsetsInitializer offsets =
 *                   OffsetsInitializer.timestamp(timestamp);
 *               KafkaSource<String> kafkaSource =
 *                   KafkaSource.<String>builder()
 *                       .setBootstrapServers("localhost:9092")
 *                       .setGroupId("MyGroup")
 *                       .setTopics(Arrays.asList("quickstart-events"))
 *                       .setDeserializer(
 *                           KafkaRecordDeserializer.valueOnly(StringDeserializer.class))
 *                       .setStartingOffsets(offsets)
 *                       .build();
 *               return kafkaSource;
 *             },
 *             Boundedness.CONTINUOUS_UNBOUNDED)
 *         .build();
 * }</pre>
 */
@PublicEvolving
public class HybridSource<T> implements Source<T, HybridSourceSplit, HybridSourceEnumeratorState> {

    private final List<SourceListEntry> sources;

    /** Protected for subclass, use {@link #builder(Source)} to construct source. */
    protected HybridSource(List<SourceListEntry> sources) {
        Preconditions.checkArgument(!sources.isEmpty());
        this.sources = sources;
    }

    /** Builder for {@link HybridSource}. */
    public static <T, EnumT extends SplitEnumerator> HybridSourceBuilder<T, EnumT> builder(
            Source<T, ?, ?> firstSource) {
        HybridSourceBuilder<T, EnumT> builder = new HybridSourceBuilder<>();
        return builder.addSource(firstSource);
    }

    @Override
    public Boundedness getBoundedness() {
        return sources.get(sources.size() - 1).boundedness;
    }

    @Override
    public SourceReader<T, HybridSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new HybridSourceReader(readerContext);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, HybridSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> enumContext) {
        return new HybridSourceSplitEnumerator(enumContext, sources, 0, null);
    }

    @Override
    public SplitEnumerator<HybridSourceSplit, HybridSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<HybridSourceSplit> enumContext,
            HybridSourceEnumeratorState checkpoint)
            throws Exception {
        return new HybridSourceSplitEnumerator(
                enumContext, sources, checkpoint.getCurrentSourceIndex(), checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceSplit> getSplitSerializer() {
        return new HybridSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<HybridSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new HybridSourceEnumeratorStateSerializer();
    }

    /**
     * Context provided to source factory.
     *
     * <p>To derive a start position at switch time, the source can be initialized from context of
     * the previous enumerator. A specific enumerator implementation may carry state such as an end
     * timestamp, that can be used to derive the start position of the next source.
     *
     * <p>Currently only the previous enumerator is exposed. The context interface allows for
     * backward compatible extension, i.e. additional information about the previous source can be
     * supplied in the future.
     */
    public interface SourceSwitchContext<EnumT> {
        EnumT getPreviousEnumerator();
    }

    /**
     * Factory for underlying sources of {@link HybridSource}.
     *
     * <p>This factory permits building of a source at graph construction time or deferred at switch
     * time. Provides the ability to set a start position in any way a specific source allows.
     * Future convenience could be built on top of it, for example a default implementation that
     * recognizes optional interfaces to transfer position in a universal format.
     *
     * <p>Called when the current enumerator has finished. The previous source's final state can
     * thus be used to construct the next source, as required for dynamic position transfer at time
     * of switching.
     *
     * <p>If start position is known at job submission time, the source can be constructed in the
     * entry point and simply wrapped into the factory, providing the benefit of validation during
     * submission.
     */
    @FunctionalInterface
    public interface SourceFactory<
                    T, SourceT extends Source<T, ?, ?>, FromEnumT extends SplitEnumerator>
            extends Serializable {
        SourceT create(SourceSwitchContext<FromEnumT> context);
    }

    private static class PassthroughSourceFactory<
                    T, SourceT extends Source<T, ?, ?>, FromEnumT extends SplitEnumerator>
            implements SourceFactory<T, SourceT, FromEnumT> {

        private final SourceT source;

        private PassthroughSourceFactory(SourceT source) {
            this.source = source;
        }

        @Override
        public SourceT create(SourceSwitchContext<FromEnumT> context) {
            return source;
        }
    }

    /** Entry for list of underlying sources. */
    static class SourceListEntry implements Serializable {
        protected final SourceFactory factory;
        protected final Boundedness boundedness;

        private SourceListEntry(SourceFactory factory, Boundedness boundedness) {
            this.factory = Preconditions.checkNotNull(factory);
            this.boundedness = Preconditions.checkNotNull(boundedness);
        }

        static SourceListEntry of(SourceFactory configurer, Boundedness boundedness) {
            return new SourceListEntry(configurer, boundedness);
        }
    }

    /** Builder for HybridSource. */
    public static class HybridSourceBuilder<T, EnumT extends SplitEnumerator>
            implements Serializable {
        private final List<SourceListEntry> sources;

        public HybridSourceBuilder() {
            sources = new ArrayList<>();
        }

        /** Add pre-configured source (without switch time modification). */
        public <ToEnumT extends SplitEnumerator, NextSourceT extends Source<T, ?, ?>>
                HybridSourceBuilder<T, ToEnumT> addSource(NextSourceT source) {
            return addSource(new PassthroughSourceFactory<>(source), source.getBoundedness());
        }

        /** Add source with deferred instantiation based on previous enumerator. */
        public <ToEnumT extends SplitEnumerator, NextSourceT extends Source<T, ?, ?>>
                HybridSourceBuilder<T, ToEnumT> addSource(
                        SourceFactory<T, NextSourceT, ? super EnumT> sourceFactory,
                        Boundedness boundedness) {
            if (!sources.isEmpty()) {
                Preconditions.checkArgument(
                        Boundedness.BOUNDED.equals(sources.get(sources.size() - 1).boundedness),
                        "All sources except the final source need to be bounded.");
            }
            ClosureCleaner.clean(
                    sourceFactory, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            sources.add(SourceListEntry.of(sourceFactory, boundedness));
            return (HybridSourceBuilder) this;
        }

        /** Build the source. */
        public HybridSource<T> build() {
            return new HybridSource(sources);
        }
    }
}
