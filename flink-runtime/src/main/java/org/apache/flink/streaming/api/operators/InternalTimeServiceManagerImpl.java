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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTaskCancellationContext;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An entity keeping all the time-related services. Right now, this is only a {@link
 * InternalTimerServiceImpl timer services}.
 *
 * <p><b>NOTE:</b> These services are only available to keyed operators.
 *
 * @param <K> The type of keys used for the timers and the registry.
 */
@Internal
public class InternalTimeServiceManagerImpl<K> implements InternalTimeServiceManager<K> {
    protected static final Logger LOG =
            LoggerFactory.getLogger(InternalTimeServiceManagerImpl.class);

    @VisibleForTesting static final String TIMER_STATE_PREFIX = "_timer_state";

    @VisibleForTesting
    static final String PROCESSING_TIMER_PREFIX = TIMER_STATE_PREFIX + "/processing_";

    @VisibleForTesting static final String EVENT_TIMER_PREFIX = TIMER_STATE_PREFIX + "/event_";

    private final TaskIOMetricGroup taskIOMetricGroup;
    private final KeyGroupRange localKeyGroupRange;
    private final KeyContext keyContext;
    private final PriorityQueueSetFactory priorityQueueSetFactory;
    private final ProcessingTimeService processingTimeService;
    private final StreamTaskCancellationContext cancellationContext;

    private final Map<String, InternalTimerServiceImpl<K, ?>> timerServices;

    private InternalTimeServiceManagerImpl(
            TaskIOMetricGroup taskIOMetricGroup,
            KeyGroupRange localKeyGroupRange,
            KeyContext keyContext,
            PriorityQueueSetFactory priorityQueueSetFactory,
            ProcessingTimeService processingTimeService,
            StreamTaskCancellationContext cancellationContext) {
        this.taskIOMetricGroup = taskIOMetricGroup;
        this.localKeyGroupRange = Preconditions.checkNotNull(localKeyGroupRange);
        this.priorityQueueSetFactory = Preconditions.checkNotNull(priorityQueueSetFactory);
        this.keyContext = Preconditions.checkNotNull(keyContext);
        this.processingTimeService = Preconditions.checkNotNull(processingTimeService);
        this.cancellationContext = cancellationContext;

        this.timerServices = new HashMap<>();
    }

    /**
     * A factory method for creating the {@link InternalTimeServiceManagerImpl}.
     *
     * <p><b>IMPORTANT:</b> Keep in sync with {@link InternalTimeServiceManager.Provider}.
     */
    public static <K> InternalTimeServiceManagerImpl<K> create(
            TaskIOMetricGroup taskIOMetricGroup,
            PriorityQueueSetFactory factory,
            KeyGroupRange keyGroupRange,
            ClassLoader userClassloader,
            KeyContext keyContext,
            ProcessingTimeService processingTimeService,
            Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStates,
            StreamTaskCancellationContext cancellationContext)
            throws Exception {

        final InternalTimeServiceManagerImpl<K> timeServiceManager =
                new InternalTimeServiceManagerImpl<>(
                        taskIOMetricGroup,
                        keyGroupRange,
                        keyContext,
                        factory,
                        processingTimeService,
                        cancellationContext);

        // and then initialize the timer services
        for (KeyGroupStatePartitionStreamProvider streamProvider : rawKeyedStates) {
            int keyGroupIdx = streamProvider.getKeyGroupId();

            Preconditions.checkArgument(
                    keyGroupRange.contains(keyGroupIdx),
                    "Key Group " + keyGroupIdx + " does not belong to the local range.");

            timeServiceManager.restoreStateForKeyGroup(
                    streamProvider.getStream(), keyGroupIdx, userClassloader);
        }

        return timeServiceManager;
    }

    @Override
    public <N> InternalTimerService<N> getInternalTimerService(
            String name,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            Triggerable<K, N> triggerable) {
        checkNotNull(keySerializer, "Timers can only be used on keyed operators.");

        // the following casting is to overcome type restrictions.
        TimerSerializer<K, N> timerSerializer =
                new TimerSerializer<>(keySerializer, namespaceSerializer);

        InternalTimerServiceImpl<K, N> timerService =
                registerOrGetTimerService(name, timerSerializer);

        timerService.startTimerService(
                timerSerializer.getKeySerializer(),
                timerSerializer.getNamespaceSerializer(),
                triggerable);

        return timerService;
    }

    @SuppressWarnings("unchecked")
    <N> InternalTimerServiceImpl<K, N> registerOrGetTimerService(
            String name, TimerSerializer<K, N> timerSerializer) {
        InternalTimerServiceImpl<K, N> timerService =
                (InternalTimerServiceImpl<K, N>) timerServices.get(name);
        if (timerService == null) {

            timerService =
                    new InternalTimerServiceImpl<>(
                            taskIOMetricGroup,
                            localKeyGroupRange,
                            keyContext,
                            processingTimeService,
                            createTimerPriorityQueue(
                                    PROCESSING_TIMER_PREFIX + name, timerSerializer),
                            createTimerPriorityQueue(EVENT_TIMER_PREFIX + name, timerSerializer),
                            cancellationContext);

            timerServices.put(name, timerService);
        }
        return timerService;
    }

    @Override
    public <N> InternalTimerService<N> getAsyncInternalTimerService(
            String name,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            Triggerable<K, N> triggerable,
            AsyncExecutionController<K> asyncExecutionController) {
        checkNotNull(keySerializer, "Timers can only be used on keyed operators.");

        // the following casting is to overcome type restrictions.
        TimerSerializer<K, N> timerSerializer =
                new TimerSerializer<>(keySerializer, namespaceSerializer);

        InternalTimerServiceAsyncImpl<K, N> timerService =
                registerOrGetAsyncTimerService(name, timerSerializer, asyncExecutionController);

        timerService.startTimerService(
                timerSerializer.getKeySerializer(),
                timerSerializer.getNamespaceSerializer(),
                triggerable);

        return timerService;
    }

    <N> InternalTimerServiceAsyncImpl<K, N> registerOrGetAsyncTimerService(
            String name,
            TimerSerializer<K, N> timerSerializer,
            AsyncExecutionController<K> asyncExecutionController) {
        InternalTimerServiceAsyncImpl<K, N> timerService =
                (InternalTimerServiceAsyncImpl<K, N>) timerServices.get(name);
        if (timerService == null) {
            timerService =
                    new InternalTimerServiceAsyncImpl<>(
                            taskIOMetricGroup,
                            localKeyGroupRange,
                            keyContext,
                            processingTimeService,
                            createTimerPriorityQueue(
                                    PROCESSING_TIMER_PREFIX + name, timerSerializer),
                            createTimerPriorityQueue(EVENT_TIMER_PREFIX + name, timerSerializer),
                            cancellationContext,
                            asyncExecutionController);

            timerServices.put(name, timerService);
        }
        return timerService;
    }

    Map<String, InternalTimerServiceImpl<K, ?>> getRegisteredTimerServices() {
        return Collections.unmodifiableMap(timerServices);
    }

    private <N>
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> createTimerPriorityQueue(
                    String name, TimerSerializer<K, N> timerSerializer) {
        return priorityQueueSetFactory.create(name, timerSerializer);
    }

    @Override
    public void advanceWatermark(Watermark watermark) throws Exception {
        for (InternalTimerServiceImpl<?, ?> service : timerServices.values()) {
            service.advanceWatermark(watermark.getTimestamp());
        }
    }

    @Override
    public boolean tryAdvanceWatermark(
            Watermark watermark, ShouldStopAdvancingFn shouldStopAdvancingFn) throws Exception {
        for (InternalTimerServiceImpl<?, ?> service : timerServices.values()) {
            if (!service.tryAdvanceWatermark(watermark.getTimestamp(), shouldStopAdvancingFn)) {
                return false;
            }
        }
        return true;
    }

    //////////////////				Fault Tolerance Methods				///////////////////

    @Override
    public void snapshotToRawKeyedState(KeyedStateCheckpointOutputStream out, String operatorName)
            throws Exception {
        try {
            KeyGroupsList allKeyGroups = out.getKeyGroupList();
            for (int keyGroupIdx : allKeyGroups) {
                out.startNewKeyGroup(keyGroupIdx);

                snapshotStateForKeyGroup(new DataOutputViewStreamWrapper(out), keyGroupIdx);
            }
        } catch (Exception exception) {
            throw new Exception(
                    "Could not write timer service of "
                            + operatorName
                            + " to checkpoint state stream.",
                    exception);
        } finally {
            try {
                out.close();
            } catch (Exception closeException) {
                LOG.warn(
                        "Could not close raw keyed operator state stream for {}. This "
                                + "might have prevented deleting some state data.",
                        operatorName,
                        closeException);
            }
        }
    }

    private void snapshotStateForKeyGroup(DataOutputView stream, int keyGroupIdx)
            throws IOException {
        InternalTimerServiceSerializationProxy<K> serializationProxy =
                new InternalTimerServiceSerializationProxy<>(this, keyGroupIdx);

        serializationProxy.write(stream);
    }

    private void restoreStateForKeyGroup(
            InputStream stream, int keyGroupIdx, ClassLoader userCodeClassLoader)
            throws IOException {

        InternalTimerServiceSerializationProxy<K> serializationProxy =
                new InternalTimerServiceSerializationProxy<>(
                        this, userCodeClassLoader, keyGroupIdx);

        serializationProxy.read(stream);
    }

    ////////////////////			Methods used ONLY IN TESTS				////////////////////

    @VisibleForTesting
    public int numProcessingTimeTimers() {
        int count = 0;
        for (InternalTimerServiceImpl<?, ?> timerService : timerServices.values()) {
            count += timerService.numProcessingTimeTimers();
        }
        return count;
    }

    @VisibleForTesting
    public int numEventTimeTimers() {
        int count = 0;
        for (InternalTimerServiceImpl<?, ?> timerService : timerServices.values()) {
            count += timerService.numEventTimeTimers();
        }
        return count;
    }
}
