/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.asyncprocessing.operators.co;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.asyncprocessing.declare.DeclaredVariable;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateUdfStreamOperator;
import org.apache.flink.runtime.asyncprocessing.operators.TimestampedCollectorWithDeclaredVariable;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.co.IntervalJoinOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * An {@link TwoInputStreamOperator operator} to execute time-bounded stream inner joins. This is
 * the async state access version of {@link IntervalJoinOperator}.
 *
 * <p>By using a configurable lower and upper bound this operator will emit exactly those pairs (T1,
 * T2) where t2.ts ∈ [T1.ts + lowerBound, T1.ts + upperBound]. Both the lower and the upper bound
 * can be configured to be either inclusive or exclusive.
 *
 * <p>As soon as elements are joined they are passed to a user-defined {@link ProcessJoinFunction}.
 *
 * <p>The basic idea of this implementation is as follows: Whenever we receive an element at {@link
 * #processElement1(StreamRecord)} (a.k.a. the left side), we add it to the left buffer. We then
 * check the right buffer to see whether there are any elements that can be joined. If there are,
 * they are joined and passed to the aforementioned function. The same happens the other way around
 * when receiving an element on the right side.
 *
 * <p>Whenever a pair of elements is emitted it will be assigned the max timestamp of either of the
 * elements.
 *
 * <p>In order to avoid the element buffers to grow indefinitely a cleanup timer is registered per
 * element. This timer indicates when an element is not considered for joining anymore and can be
 * removed from the state.
 *
 * @param <K> The type of the key based on which we join elements.
 * @param <T1> The type of the elements in the left stream.
 * @param <T2> The type of the elements in the right stream.
 * @param <OUT> The output type created by the user-defined function.
 */
@Internal
public class AsyncIntervalJoinOperator<K, T1, T2, OUT>
        extends AbstractAsyncStateUdfStreamOperator<OUT, ProcessJoinFunction<T1, T2, OUT>>
        implements TwoInputStreamOperator<T1, T2, OUT>, Triggerable<K, String> {

    private static final long serialVersionUID = -5380774605111543477L;

    private static final Logger logger = LoggerFactory.getLogger(AsyncIntervalJoinOperator.class);

    private static final String LEFT_BUFFER = "LEFT_BUFFER";
    private static final String RIGHT_BUFFER = "RIGHT_BUFFER";
    private static final String CLEANUP_TIMER_NAME = "CLEANUP_TIMER";
    private static final String CLEANUP_NAMESPACE_LEFT = "CLEANUP_LEFT";
    private static final String CLEANUP_NAMESPACE_RIGHT = "CLEANUP_RIGHT";

    private final long lowerBound;
    private final long upperBound;
    private final OutputTag<T1> leftLateDataOutputTag;
    private final OutputTag<T2> rightLateDataOutputTag;
    private final TypeSerializer<T1> leftTypeSerializer;
    private final TypeSerializer<T2> rightTypeSerializer;

    private transient MapState<Long, List<IntervalJoinOperator.BufferEntry<T1>>> leftBuffer;
    private transient MapState<Long, List<IntervalJoinOperator.BufferEntry<T2>>> rightBuffer;

    // Shared timestamp variable for collector and context.
    private transient DeclaredVariable<Long> resultTimestamp;
    private transient DeclaredVariable<Long> leftTimestamp;
    private transient DeclaredVariable<Long> rightTimestamp;

    private transient TimestampedCollectorWithDeclaredVariable<OUT> collector;
    private transient ContextImpl context;

    private transient InternalTimerService<String> internalTimerService;

    /**
     * Creates a new IntervalJoinOperator.
     *
     * @param lowerBound The lower bound for evaluating if elements should be joined
     * @param upperBound The upper bound for evaluating if elements should be joined
     * @param lowerBoundInclusive Whether or not to include elements where the timestamp matches the
     *     lower bound
     * @param upperBoundInclusive Whether or not to include elements where the timestamp matches the
     *     upper bound
     * @param udf A user-defined {@link ProcessJoinFunction} that gets called whenever two elements
     *     of T1 and T2 are joined
     */
    public AsyncIntervalJoinOperator(
            long lowerBound,
            long upperBound,
            boolean lowerBoundInclusive,
            boolean upperBoundInclusive,
            OutputTag<T1> leftLateDataOutputTag,
            OutputTag<T2> rightLateDataOutputTag,
            TypeSerializer<T1> leftTypeSerializer,
            TypeSerializer<T2> rightTypeSerializer,
            ProcessJoinFunction<T1, T2, OUT> udf) {

        super(Preconditions.checkNotNull(udf));

        Preconditions.checkArgument(
                lowerBound <= upperBound, "lowerBound <= upperBound must be fulfilled");

        // Move buffer by +1 / -1 depending on inclusiveness in order not needing
        // to check for inclusiveness later on
        this.lowerBound = (lowerBoundInclusive) ? lowerBound : lowerBound + 1L;
        this.upperBound = (upperBoundInclusive) ? upperBound : upperBound - 1L;

        this.leftLateDataOutputTag = leftLateDataOutputTag;
        this.rightLateDataOutputTag = rightLateDataOutputTag;
        this.leftTypeSerializer = Preconditions.checkNotNull(leftTypeSerializer);
        this.rightTypeSerializer = Preconditions.checkNotNull(rightTypeSerializer);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.leftBuffer =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        LEFT_BUFFER,
                                        LongSerializer.INSTANCE,
                                        new ListSerializer<>(
                                                new IntervalJoinOperator.BufferEntrySerializer<>(
                                                        leftTypeSerializer))));

        this.rightBuffer =
                getRuntimeContext()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        RIGHT_BUFFER,
                                        LongSerializer.INSTANCE,
                                        new ListSerializer<>(
                                                new IntervalJoinOperator.BufferEntrySerializer<>(
                                                        rightTypeSerializer))));

        resultTimestamp =
                declarationContext.declareVariable(
                        LongSerializer.INSTANCE,
                        "_AsyncIntervalJoinOperator$resultTime",
                        () -> Long.MIN_VALUE);
        leftTimestamp =
                declarationContext.declareVariable(
                        LongSerializer.INSTANCE,
                        "_AsyncIntervalJoinOperator$leftTime",
                        () -> Long.MIN_VALUE);
        rightTimestamp =
                declarationContext.declareVariable(
                        LongSerializer.INSTANCE,
                        "_AsyncIntervalJoinOperator$rightTime",
                        () -> Long.MIN_VALUE);

        collector = new TimestampedCollectorWithDeclaredVariable<>(output, resultTimestamp);
        context = new ContextImpl(userFunction, resultTimestamp, leftTimestamp, rightTimestamp);
        internalTimerService =
                getInternalTimerService(CLEANUP_TIMER_NAME, StringSerializer.INSTANCE, this);
    }

    /**
     * Process a {@link StreamRecord} from the left stream. Whenever an {@link StreamRecord} arrives
     * at the left stream, it will get added to the left buffer. Possible join candidates for that
     * element will be looked up from the right buffer and if the pair lies within the user defined
     * boundaries, it gets passed to the {@link ProcessJoinFunction}.
     *
     * @param record An incoming record to be joined
     * @throws Exception Can throw an Exception during state access
     */
    @Override
    public void processElement1(StreamRecord<T1> record) throws Exception {
        processElement(record, leftBuffer, rightBuffer, lowerBound, upperBound, true);
    }

    /**
     * Process a {@link StreamRecord} from the right stream. Whenever a {@link StreamRecord} arrives
     * at the right stream, it will get added to the right buffer. Possible join candidates for that
     * element will be looked up from the left buffer and if the pair lies within the user defined
     * boundaries, it gets passed to the {@link ProcessJoinFunction}.
     *
     * @param record An incoming record to be joined
     * @throws Exception Can throw an exception during state access
     */
    @Override
    public void processElement2(StreamRecord<T2> record) throws Exception {
        processElement(record, rightBuffer, leftBuffer, -upperBound, -lowerBound, false);
    }

    @SuppressWarnings("unchecked")
    private <THIS, OTHER> void processElement(
            final StreamRecord<THIS> record,
            final MapState<Long, List<IntervalJoinOperator.BufferEntry<THIS>>> ourBuffer,
            final MapState<Long, List<IntervalJoinOperator.BufferEntry<OTHER>>> otherBuffer,
            final long relativeLowerBound,
            final long relativeUpperBound,
            final boolean isLeft)
            throws Exception {

        final THIS ourValue = record.getValue();
        final long ourTimestamp = record.getTimestamp();

        if (ourTimestamp == Long.MIN_VALUE) {
            throw new FlinkException(
                    "Long.MIN_VALUE timestamp: Elements used in "
                            + "interval stream joins need to have timestamps meaningful timestamps.");
        }

        if (isLate(ourTimestamp)) {
            sideOutput(ourValue, ourTimestamp, isLeft);
            return;
        }

        addToBuffer(ourBuffer, ourValue, ourTimestamp)
                .thenCompose((empty) -> otherBuffer.asyncEntries())
                .thenCompose(
                        entries ->
                                entries.onNext(
                                        bucket -> {
                                            final long timestamp = bucket.getKey();

                                            if (timestamp < ourTimestamp + relativeLowerBound
                                                    || timestamp
                                                            > ourTimestamp + relativeUpperBound) {
                                                return;
                                            }

                                            for (IntervalJoinOperator.BufferEntry<OTHER> entry :
                                                    bucket.getValue()) {
                                                if (isLeft) {
                                                    collect(
                                                            (T1) ourValue,
                                                            (T2) entry.getElement(),
                                                            ourTimestamp,
                                                            timestamp);
                                                } else {
                                                    collect(
                                                            (T1) entry.getElement(),
                                                            (T2) ourValue,
                                                            timestamp,
                                                            ourTimestamp);
                                                }
                                            }
                                        }))
                .thenAccept(
                        empty -> {
                            long cleanupTime =
                                    (relativeUpperBound > 0L)
                                            ? ourTimestamp + relativeUpperBound
                                            : ourTimestamp;
                            if (isLeft) {
                                internalTimerService.registerEventTimeTimer(
                                        CLEANUP_NAMESPACE_LEFT, cleanupTime);
                            } else {
                                internalTimerService.registerEventTimeTimer(
                                        CLEANUP_NAMESPACE_RIGHT, cleanupTime);
                            }
                        });
    }

    private boolean isLate(long timestamp) {
        long currentWatermark = internalTimerService.currentWatermark();
        return timestamp < currentWatermark;
    }

    /** Write skipped late arriving element to SideOutput. */
    protected <T> void sideOutput(T value, long timestamp, boolean isLeft) {
        if (isLeft) {
            if (leftLateDataOutputTag != null) {
                output.collect(leftLateDataOutputTag, new StreamRecord<>((T1) value, timestamp));
            }
        } else {
            if (rightLateDataOutputTag != null) {
                output.collect(rightLateDataOutputTag, new StreamRecord<>((T2) value, timestamp));
            }
        }
    }

    private void collect(T1 left, T2 right, long leftTime, long rightTime) throws Exception {
        resultTimestamp.set(Math.max(leftTime, rightTime));
        leftTimestamp.set(leftTime);
        rightTimestamp.set(rightTime);

        userFunction.processElement(left, right, context, collector);
    }

    private static <T> StateFuture<Void> addToBuffer(
            final MapState<Long, List<IntervalJoinOperator.BufferEntry<T>>> buffer,
            final T value,
            final long timestamp) {
        return buffer.asyncGet(timestamp)
                .thenCompose(
                        (elemsInBucket) -> {
                            if (elemsInBucket == null) {
                                elemsInBucket = new ArrayList<>();
                            }
                            elemsInBucket.add(new IntervalJoinOperator.BufferEntry<>(value, false));
                            return buffer.asyncPut(timestamp, elemsInBucket);
                        });
    }

    @Override
    public void onEventTime(InternalTimer<K, String> timer) throws Exception {
        long timerTimestamp = timer.getTimestamp();
        String namespace = timer.getNamespace();

        logger.trace("onEventTime @ {}", timerTimestamp);

        switch (namespace) {
            case CLEANUP_NAMESPACE_LEFT:
                {
                    long timestamp =
                            (upperBound <= 0L) ? timerTimestamp : timerTimestamp - upperBound;
                    logger.trace("Removing from left buffer @ {}", timestamp);
                    leftBuffer.remove(timestamp);
                    break;
                }
            case CLEANUP_NAMESPACE_RIGHT:
                {
                    long timestamp =
                            (lowerBound <= 0L) ? timerTimestamp + lowerBound : timerTimestamp;
                    logger.trace("Removing from right buffer @ {}", timestamp);
                    rightBuffer.remove(timestamp);
                    break;
                }
            default:
                throw new RuntimeException("Invalid namespace " + namespace);
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<K, String> timer) throws Exception {
        // do nothing.
    }

    /**
     * The context that is available during an invocation of {@link
     * ProcessJoinFunction#processElement(Object, Object, ProcessJoinFunction.Context, Collector)}.
     *
     * <p>It gives access to the timestamps of the left element in the joined pair, the right one,
     * and that of the joined pair. In addition, this context allows to emit elements on a side
     * output.
     */
    private final class ContextImpl extends ProcessJoinFunction<T1, T2, OUT>.Context {

        private final DeclaredVariable<Long> resultTimestamp;

        private final DeclaredVariable<Long> leftTimestamp;

        private final DeclaredVariable<Long> rightTimestamp;

        private ContextImpl(
                ProcessJoinFunction<T1, T2, OUT> func,
                DeclaredVariable<Long> resultTimestamp,
                DeclaredVariable<Long> leftTimestamp,
                DeclaredVariable<Long> rightTimestamp) {
            func.super();
            this.resultTimestamp = resultTimestamp;
            this.leftTimestamp = leftTimestamp;
            this.rightTimestamp = rightTimestamp;
        }

        @Override
        public long getLeftTimestamp() {
            return leftTimestamp.get();
        }

        @Override
        public long getRightTimestamp() {
            return rightTimestamp.get();
        }

        @Override
        public long getTimestamp() {
            return resultTimestamp.get();
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            Preconditions.checkArgument(outputTag != null, "OutputTag must not be null");
            output.collect(outputTag, new StreamRecord<>(value, getTimestamp()));
        }
    }

    @VisibleForTesting
    public MapState<Long, List<IntervalJoinOperator.BufferEntry<T1>>> getLeftBuffer() {
        return leftBuffer;
    }

    @VisibleForTesting
    public MapState<Long, List<IntervalJoinOperator.BufferEntry<T2>>> getRightBuffer() {
        return rightBuffer;
    }
}
