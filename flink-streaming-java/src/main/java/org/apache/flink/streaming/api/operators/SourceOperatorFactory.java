/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorProvider;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.streaming.runtime.tasks.StreamTask.CanEmitBatchOfRecordsChecker;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The Factory class for {@link SourceOperator}. */
public class SourceOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT>
        implements CoordinatedOperatorFactory<OUT>, ProcessingTimeServiceAware {

    private static final long serialVersionUID = 1L;

    /** The {@link Source} to create the {@link SourceOperator}. */
    private final Source<OUT, ?, ?> source;

    /** The event time setup (timestamp assigners, watermark generators, etc.). */
    private final WatermarkStrategy<OUT> watermarkStrategy;

    /** Whether to emit intermediate watermarks or only one final watermark at the end of input. */
    private final boolean emitProgressiveWatermarks;

    /** The number of worker thread for the source coordinator. */
    private final int numCoordinatorWorkerThread;

    private @Nullable String coordinatorListeningID;

    public SourceOperatorFactory(
            Source<OUT, ?, ?> source, WatermarkStrategy<OUT> watermarkStrategy) {
        this(source, watermarkStrategy, true /* emit progressive watermarks */, 1);
    }

    public SourceOperatorFactory(
            Source<OUT, ?, ?> source,
            WatermarkStrategy<OUT> watermarkStrategy,
            boolean emitProgressiveWatermarks) {
        this(source, watermarkStrategy, emitProgressiveWatermarks, 1);
    }

    public SourceOperatorFactory(
            Source<OUT, ?, ?> source,
            WatermarkStrategy<OUT> watermarkStrategy,
            boolean emitProgressiveWatermarks,
            int numCoordinatorWorkerThread) {
        this.source = checkNotNull(source);
        this.watermarkStrategy = checkNotNull(watermarkStrategy);
        this.emitProgressiveWatermarks = emitProgressiveWatermarks;
        this.numCoordinatorWorkerThread = numCoordinatorWorkerThread;
    }

    public Boundedness getBoundedness() {
        return source.getBoundedness();
    }

    public void setCoordinatorListeningID(@Nullable String coordinatorListeningID) {
        this.coordinatorListeningID = coordinatorListeningID;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
        final OperatorEventGateway gateway =
                parameters.getOperatorEventDispatcher().getOperatorEventGateway(operatorId);

        final SourceOperator<OUT, ?> sourceOperator =
                instantiateSourceOperator(
                        source::createReader,
                        gateway,
                        source.getSplitSerializer(),
                        watermarkStrategy,
                        parameters.getProcessingTimeService(),
                        parameters
                                .getContainingTask()
                                .getEnvironment()
                                .getTaskManagerInfo()
                                .getConfiguration(),
                        parameters
                                .getContainingTask()
                                .getEnvironment()
                                .getTaskManagerInfo()
                                .getTaskManagerExternalAddress(),
                        emitProgressiveWatermarks,
                        parameters.getContainingTask().getCanEmitBatchOfRecords());

        sourceOperator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        parameters.getOperatorEventDispatcher().registerEventHandler(operatorId, sourceOperator);

        // today's lunch is generics spaghetti
        @SuppressWarnings("unchecked")
        final T castedOperator = (T) sourceOperator;

        return castedOperator;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new SourceCoordinatorProvider<>(
                operatorName,
                operatorID,
                source,
                numCoordinatorWorkerThread,
                watermarkStrategy.getAlignmentParameters(),
                coordinatorListeningID);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return SourceOperator.class;
    }

    @Override
    public boolean isStreamSource() {
        return true;
    }

    /**
     * This is a utility method to conjure up a "SplitT" generics variable binding so that we can
     * construct the SourceOperator without resorting to "all raw types". That way, this methods
     * puts all "type non-safety" in one place and allows to maintain as much generics safety in the
     * main code as possible.
     */
    @SuppressWarnings("unchecked")
    private static <T, SplitT extends SourceSplit>
            SourceOperator<T, SplitT> instantiateSourceOperator(
                    FunctionWithException<SourceReaderContext, SourceReader<T, ?>, Exception>
                            readerFactory,
                    OperatorEventGateway eventGateway,
                    SimpleVersionedSerializer<?> splitSerializer,
                    WatermarkStrategy<T> watermarkStrategy,
                    ProcessingTimeService timeService,
                    Configuration config,
                    String localHostName,
                    boolean emitProgressiveWatermarks,
                    CanEmitBatchOfRecordsChecker canEmitBatchOfRecords) {

        // jumping through generics hoops: cast the generics away to then cast them back more
        // strictly typed
        final FunctionWithException<SourceReaderContext, SourceReader<T, SplitT>, Exception>
                typedReaderFactory =
                        (FunctionWithException<
                                        SourceReaderContext, SourceReader<T, SplitT>, Exception>)
                                (FunctionWithException<?, ?, ?>) readerFactory;

        final SimpleVersionedSerializer<SplitT> typedSplitSerializer =
                (SimpleVersionedSerializer<SplitT>) splitSerializer;

        return new SourceOperator<>(
                typedReaderFactory,
                eventGateway,
                typedSplitSerializer,
                watermarkStrategy,
                timeService,
                config,
                localHostName,
                emitProgressiveWatermarks,
                canEmitBatchOfRecords);
    }
}
