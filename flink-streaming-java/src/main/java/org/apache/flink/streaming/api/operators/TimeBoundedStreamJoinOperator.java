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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.functions.TimeBoundedJoinFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A TwoInputStreamOperator to execute time-bounded stream inner joins.
 *
 * <p>By using a configurable lower and upper bound this operator will emit exactly those pairs
 * (T1, T2) where t2.ts ∈ [T1.ts + lowerBound, T1.ts + upperBound]. Both the lower and the
 * upper bound can be configured to be either inclusive or exclusive.
 *
 * <p>As soon as elements are joined they are passed to a user-defined {@link TimeBoundedJoinFunction},
 * as a {@link Tuple2}, with f0 being the left element and f1 being the right element
 *
 * <p>The basic idea of this implementation is as follows: Whenever we receive an element at
 * {@link #processElement1(StreamRecord)} (a.k.a. the left side), we add it to the left buffer.
 * We then check the right buffer to see whether there are any elements that can be joined. If
 * there are, they are joined and passed to a user-defined {@link TimeBoundedJoinFunction}.
 * The same happens the other way around when receiving an element on the right side.
 *
 * <p>In some cases the watermark needs to be delayed. This for example can happen if
 * if t2.ts ∈ [t1.ts + 1, t1.ts + 2] and elements from t1 arrive earlier than elements from t2 and
 * therefore get added to the left buffer. When an element now arrives on the right side, the
 * watermark might have already progressed. The right element now gets joined with an
 * older element from the left side, where the timestamp of the left element is lower than the
 * current watermark, which would make this element late. This can be avoided by holding back the
 * watermarks.
 *
 * <p>The left and right buffers are cleared from unused values periodically
 * (triggered by watermarks) in order not to grow infinitely.
 *
 *
 * @param <T1> The type of the elements in the left stream
 * @param <T2> The type of the elements in the right stream
 * @param <OUT> The output type created by the user-defined function
 */
@Internal
public class TimeBoundedStreamJoinOperator<K, T1, T2, OUT>
	extends AbstractUdfStreamOperator<OUT, TimeBoundedJoinFunction<T1, T2, OUT>>
	implements TwoInputStreamOperator<T1, T2, OUT>, Triggerable<K, VoidNamespace> {

	private static final String LEFT_BUFFER = "LEFT_BUFFER";
	private static final String RIGHT_BUFFER = "RIGHT_BUFFER";
	private static final String CLEANUP_TIMER_NAME = "CLEANUP_TIMER";

	private final long lowerBound;
	private final long upperBound;

	private final long inverseLowerBound;
	private final long inverseUpperBound;

	private final boolean lowerBoundInclusive;
	private final boolean upperBoundInclusive;

	private final TypeSerializer<T1> leftTypeSerializer;
	private final TypeSerializer<T2> rightTypeSerializer;

	private final long bucketGranularity;
	private final long watermarkDelay;

	private long lastWatermark = Long.MIN_VALUE;

	private transient MapState<Long, List<Tuple3<T1, Long, Boolean>>> leftBuffer;
	private transient MapState<Long, List<Tuple3<T2, Long, Boolean>>> rightBuffer;

	private transient TimestampedCollector<OUT> collector;
	private transient ContextImpl context;

	private transient InternalTimerService<VoidNamespace> internalTimerService;

	/**
	 * Creates a new TimeBoundedStreamJoinOperator.
	 *
	 * @param lowerBound          The lower bound for evaluating if elements should be joined
	 * @param upperBound          The upper bound for evaluating if elements should be joined
	 * @param lowerBoundInclusive Whether or not to include elements where the timestamp matches
	 *                            the lower bound
	 * @param upperBoundInclusive Whether or not to include elements where the timestamp matches
	 *                            the upper bound
	 * @param udf                 A user-defined {@link TimeBoundedJoinFunction} that gets called
	 *                            whenever two elements of T1 and T2 are joined
	 */
	public TimeBoundedStreamJoinOperator(
			long lowerBound,
			long upperBound,
			boolean lowerBoundInclusive,
			boolean upperBoundInclusive,
			long bucketGranularity,
			TypeSerializer<T1> leftTypeSerializer,
			TypeSerializer<T2> rightTypeSerializer,
			TimeBoundedJoinFunction<T1, T2, OUT> udf) {

		super(Preconditions.checkNotNull(udf));

		Preconditions.checkArgument(lowerBound <= upperBound,
			"lowerBound <= upperBound must be fulfilled");
		Preconditions.checkArgument(bucketGranularity > 0, "bucket size must be greater than zero");

		this.lowerBound = lowerBound;
		this.upperBound = upperBound;

		this.inverseLowerBound = -upperBound;
		this.inverseUpperBound = -lowerBound;

		this.lowerBoundInclusive = lowerBoundInclusive;
		this.upperBoundInclusive = upperBoundInclusive;
		this.leftTypeSerializer = Preconditions.checkNotNull(leftTypeSerializer);
		this.rightTypeSerializer = Preconditions.checkNotNull(rightTypeSerializer);

		this.bucketGranularity = bucketGranularity;
		this.watermarkDelay = (upperBound < 0) ? 0 : upperBound;
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);
		context = new ContextImpl(userFunction);
		internalTimerService =
			getInternalTimerService(CLEANUP_TIMER_NAME, VoidNamespaceSerializer.INSTANCE, this);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		@SuppressWarnings("unchecked")
		Class<Tuple3<T1, Long, Boolean>> leftTypedTuple =
			(Class<Tuple3<T1, Long, Boolean>>) (Class<?>) Tuple3.class;

		TupleSerializer<Tuple3<T1, Long, Boolean>> leftTupleSerializer = new TupleSerializer<>(
			leftTypedTuple,
			new TypeSerializer[]{
				leftTypeSerializer,
				LongSerializer.INSTANCE,
				BooleanSerializer.INSTANCE
			}
		);

		@SuppressWarnings("unchecked")
		Class<Tuple3<T2, Long, Boolean>> rightTypedTuple =
			(Class<Tuple3<T2, Long, Boolean>>) (Class<?>) Tuple3.class;

		TupleSerializer<Tuple3<T2, Long, Boolean>> rightTupleSerializer = new TupleSerializer<>(
			rightTypedTuple,
			new TypeSerializer[]{
				rightTypeSerializer,
				LongSerializer.INSTANCE,
				BooleanSerializer.INSTANCE
			}
		);

		this.leftBuffer = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>(
			LEFT_BUFFER,
			LongSerializer.INSTANCE,
			new ListSerializer<>(leftTupleSerializer)
		));

		this.rightBuffer = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>(
			RIGHT_BUFFER,
			LongSerializer.INSTANCE,
			new ListSerializer<>(rightTupleSerializer)
		));
	}

	/**
	 * Process a {@link StreamRecord} from the left stream. Whenever an {@link StreamRecord}
	 * arrives at the left stream, it will get added to the left buffer. Possible join candidates
	 * for that element will be looked up from the right buffer and if the pair lies within the
	 * user defined boundaries, it gets collected.
	 *
	 * @param record An incoming record to be joined
	 * @throws Exception Can throw an Exception during state access
	 */
	@Override
	public void processElement1(StreamRecord<T1> record) throws Exception {
		processElement(record, leftBuffer, rightBuffer, lowerBound, upperBound, true);
	}

	/**
	 * Process a {@link StreamRecord} from the right stream. Whenever a {@link StreamRecord}
	 * arrives at the right stream, it will get added to the right buffer. Possible join candidates
	 * for that element will be looked up from the left buffer and if the pair lies within the user
	 * defined boundaries, it gets collected.
	 *
	 * @param record An incoming record to be joined
	 * @throws Exception Can throw an exception during state access
	 */
	@Override
	public void processElement2(StreamRecord<T2> record) throws Exception {
		processElement(record, rightBuffer, leftBuffer, inverseLowerBound, inverseUpperBound, false);
	}

	@SuppressWarnings("unchecked")
	private <OUR, OTHER> void processElement(
		StreamRecord<OUR> record,
		MapState<Long, List<Tuple3<OUR, Long, Boolean>>> ourBuffer,
		MapState<Long, List<Tuple3<OTHER, Long, Boolean>>> otherBuffer,
		long lowerBound,
		long upperBound,
		boolean isLeft
	) throws Exception {

		OUR ourValue = record.getValue();
		long ourTimestamp = record.getTimestamp();

		long joinLowerBound = ourTimestamp + lowerBound;
		long joinUpperBound = ourTimestamp + upperBound;

		if (ourTimestamp == Long.MIN_VALUE) {
			throw new FlinkException("Time-bounded stream joins need to have timestamps " +
				"assigned to elements, but current element has timestamp Long.MIN_VALUE");
		}

		if (dataIsLate(ourTimestamp)) {
			return;
		}

		addToBuffer(ourBuffer, ourValue, ourTimestamp);

		for (Map.Entry<Long, List<Tuple3<OTHER, Long, Boolean>>> entry : otherBuffer.entries()) {
			long bucketStart = entry.getKey();
			long bucketEnd = bucketStart + bucketGranularity;

			if (!(bucketEnd >= joinLowerBound && bucketStart <= joinUpperBound)) {
				// skip buckets that are out of bounds
				continue;
			}

			List<Tuple3<OTHER, Long, Boolean>> fromBucket = entry.getValue();

			// check for each element in current bucket if it should be joined
			for (Tuple3<OTHER, Long, Boolean> tuple : fromBucket) {

				// this is the one point where we have to cast our types OUR and OTHER
				// to T1 and T2 again, which is why we need information about which side
				// we are operating on. This is passed in via 'isLeft'
				if (isLeft && shouldBeJoined(ourTimestamp, tuple.f1)) {
					collect((T1) ourValue, (T2) tuple.f0, ourTimestamp, tuple.f1);
				} else if (!isLeft && shouldBeJoined(tuple.f1, ourTimestamp)) {
					collect((T1) tuple.f0, (T2) ourValue, tuple.f1, ourTimestamp);
				}
			}
		}

		registerCleanupTimer();
	}

	private void registerCleanupTimer() {
		if (this.lastWatermark == Long.MIN_VALUE) {
			return;
		}

		long triggerTime = this.lastWatermark + 1;
		internalTimerService.registerEventTimeTimer(VoidNamespace.INSTANCE, triggerTime);
	}

	private boolean dataIsLate(long rightTs) {
		return this.lastWatermark != Long.MIN_VALUE && rightTs < lastWatermark - watermarkDelay;
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {

		// We can not clean our state here directly because we are not in a keyed context. Instead
		// we set a field containing the last watermark that we have seen, and for every element in
		// processElement1(...) / processElement2(...) we register a timer with time: watermark + 1
		// This watermark + 1 will then trigger the onEventTime(...) method for the next watermark,
		// where we are in a keyed context again, which we can use to clean up our state.
		this.lastWatermark = mark.getTimestamp();

		if (timeServiceManager != null) {
			timeServiceManager.advanceWatermark(mark);
		}

		// emit the watermark with the calculated delay, so we don't produce late data
		output.emitWatermark(new Watermark(this.lastWatermark - watermarkDelay));
	}

	private void collect(T1 left, T2 right, long leftTs, long rightTs) throws Exception {
		collector.setAbsoluteTimestamp(leftTs);
		context.leftTs = leftTs;
		context.rightTs = rightTs;
		userFunction.processElement(left, right, context, this.collector);
	}

	private <T> void removeFromBufferUntil(
		MapState<Long, List<Tuple3<T, Long, Boolean>>> buffer,
		long maxCleanup
	) throws Exception {

		Iterator<Map.Entry<Long, List<Tuple3<T, Long, Boolean>>>> iterator = buffer.iterator();
		while (iterator.hasNext()) {
			Map.Entry<Long, List<Tuple3<T, Long, Boolean>>> next = iterator.next();
			if (next.getKey() + bucketGranularity <= maxCleanup) {
				iterator.remove();
			}
		}
	}

	private boolean shouldBeJoined(long leftTs, long rightTs) {
		long elemLowerBound = leftTs + lowerBound;
		long elemUpperBound = leftTs + upperBound;

		boolean lowerBoundOk = (lowerBoundInclusive)
			? (elemLowerBound <= rightTs)
			: (elemLowerBound < rightTs);

		boolean upperBoundOk = (upperBoundInclusive)
			? (rightTs <= elemUpperBound)
			: (rightTs < elemUpperBound);

		return lowerBoundOk && upperBoundOk;
	}

	private <T> void addToBuffer(
		MapState<Long, List<Tuple3<T, Long, Boolean>>> buffer,
		T value,
		long ts
	) throws Exception {

		long bucket = calculateBucket(ts);
		Tuple3<T, Long, Boolean> elem = Tuple3.of(
			value, // actual value
			ts,    // actual timestamp
			false  // has been joined
		);

		List<Tuple3<T, Long, Boolean>> elemsInBucket = buffer.get(bucket);
		if (elemsInBucket == null) {
			elemsInBucket = new ArrayList<>();
		}
		elemsInBucket.add(elem);
		buffer.put(bucket, elemsInBucket);
	}

	private long calculateBucket(long ts) {
		return ts - (ts % bucketGranularity);
	}

	@Override
	public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
		// remove from both sides all those elements where the timestamp is less than the lower
		// bound, because they are not considered for joining anymore
		removeFromBufferUntil(leftBuffer, timer.getTimestamp() + inverseLowerBound);
		removeFromBufferUntil(rightBuffer, timer.getTimestamp() + lowerBound);
	}

	@Override
	public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
		throw new RuntimeException("Processing time is not supported for time-bounded joins");
	}

	private class ContextImpl extends TimeBoundedJoinFunction<T1, T2, OUT>.Context {

		private long leftTs;
		private long rightTs;

		private ContextImpl(TimeBoundedJoinFunction<T1, T2, OUT> func) {
			func.super();
		}

		@Override
		public long getLeftTimestamp() {
			return leftTs;
		}

		@Override
		public long getRightTimestamp() {
			return rightTs;
		}

		@Override
		public long getTimestamp() {
			return leftTs;
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			Preconditions.checkArgument(outputTag != null, "OutputTag must not be null");
			output.collect(outputTag, new StreamRecord<>(value, getTimestamp()));
		}
	}

	@VisibleForTesting
	protected MapState<Long, List<Tuple3<T1, Long, Boolean>>> getLeftBuffer() {
		return leftBuffer;
	}

	@VisibleForTesting
	protected MapState<Long, List<Tuple3<T2, Long, Boolean>>> getRightBuffer() {
		return rightBuffer;
	}
}
