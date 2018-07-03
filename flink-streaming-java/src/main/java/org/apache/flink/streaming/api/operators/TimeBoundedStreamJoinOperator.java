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
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.functions.TimeBoundedJoinFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
	implements TwoInputStreamOperator<T1, T2, OUT>, Triggerable<K, String> {

	private static final String LEFT_BUFFER = "LEFT_BUFFER";
	private static final String RIGHT_BUFFER = "RIGHT_BUFFER";
	private static final String CLEANUP_TIMER_NAME = "CLEANUP_TIMER";
	private static final String CLEANUP_NAMESPACE_LEFT = "CLEANUP_LEFT";
	private static final String CLEANUP_NAMESPACE_RIGHT = "CLEANUP_RIGHT";

	private final long lowerBound;
	private final long upperBound;

	private final TypeSerializer<T1> leftTypeSerializer;
	private final TypeSerializer<T2> rightTypeSerializer;

	private transient MapState<Long, List<BufferEntry<T1>>> leftBuffer;
	private transient MapState<Long, List<BufferEntry<T2>>> rightBuffer;

	private transient TimestampedCollector<OUT> collector;
	private transient ContextImpl context;

	private transient InternalTimerService<String> internalTimerService;
	private Logger logger = LoggerFactory.getLogger(TimeBoundedStreamJoinOperator.class);

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
			TypeSerializer<T1> leftTypeSerializer,
			TypeSerializer<T2> rightTypeSerializer,
			TimeBoundedJoinFunction<T1, T2, OUT> udf) {

		super(Preconditions.checkNotNull(udf));

		Preconditions.checkArgument(lowerBound <= upperBound,
			"lowerBound <= upperBound must be fulfilled");

		this.lowerBound = (lowerBoundInclusive) ? lowerBound : lowerBound + 1;
		this.upperBound = (upperBoundInclusive) ? upperBound : upperBound - 1;

		this.leftTypeSerializer = Preconditions.checkNotNull(leftTypeSerializer);
		this.rightTypeSerializer = Preconditions.checkNotNull(rightTypeSerializer);
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);
		context = new ContextImpl(userFunction);
		internalTimerService =
			getInternalTimerService(CLEANUP_TIMER_NAME, StringSerializer.INSTANCE, this);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		this.leftBuffer = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>(
			LEFT_BUFFER,
			LongSerializer.INSTANCE,
			new ListSerializer<>(new BufferEntrySerializer<>(leftTypeSerializer))
		));

		this.rightBuffer = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>(
			RIGHT_BUFFER,
			LongSerializer.INSTANCE,
			new ListSerializer<>(new BufferEntrySerializer<>(rightTypeSerializer))
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
		processElement(record, rightBuffer, leftBuffer, -upperBound, -lowerBound, false);
	}

	@SuppressWarnings("unchecked")
	private <OUR, OTHER> void processElement(
		StreamRecord<OUR> record,
		MapState<Long, List<BufferEntry<OUR>>> ourBuffer,
		MapState<Long, List<BufferEntry<OTHER>>> otherBuffer,
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

		for (Map.Entry<Long, List<BufferEntry<OTHER>>> entry: otherBuffer.entries()) {
			long bucket  = entry.getKey();

			if (bucket < joinLowerBound || bucket > joinUpperBound) {
				continue;
			}

			List<BufferEntry<OTHER>> fromBucket = entry.getValue();

			// check for each element in current bucket if it should be joined
			for (BufferEntry<OTHER> tuple: fromBucket) {

				// this is the one point where we have to cast our types OUR and OTHER
				// to T1 and T2 again, which is why we need information about which side
				// we are operating on. This is passed in via 'isLeft'
				if (isLeft && shouldBeJoined(ourTimestamp, tuple.timestamp)) {
					collect((T1) ourValue, (T2) tuple.element, ourTimestamp, tuple.timestamp);
				} else if (!isLeft && shouldBeJoined(tuple.timestamp, ourTimestamp)) {
					collect((T1) tuple.element, (T2) ourValue, tuple.timestamp, ourTimestamp);
				}
			}
		}


		long removalTime = calculateRemovalTime(isLeft, ourTimestamp);
		registerPerElementCleanup(isLeft, removalTime);

		if (logger.isTraceEnabled()) {
			String side = isLeft ? "left" : "right";
			logger.trace("Marked {} element @ {} for removal at {}", side, ourTimestamp, removalTime);
		}
	}

	private void registerPerElementCleanup(boolean isLeft, long timestamp) {
		if (isLeft) {
			internalTimerService.registerEventTimeTimer(CLEANUP_NAMESPACE_LEFT, timestamp);
		} else {
			internalTimerService.registerEventTimeTimer(CLEANUP_NAMESPACE_RIGHT, timestamp);
		}
	}

	private boolean dataIsLate(long rightTs) {
		long currentWatermark = internalTimerService.currentWatermark();
		return currentWatermark != Long.MIN_VALUE && rightTs < currentWatermark;
	}

	private void collect(T1 left, T2 right, long leftTs, long rightTs) throws Exception {
		long ts = Math.max(leftTs, rightTs);
		collector.setAbsoluteTimestamp(ts);
		context.leftTs = leftTs;
		context.rightTs = rightTs;
		userFunction.processElement(left, right, context, this.collector);
	}

	private boolean shouldBeJoined(long leftTs, long rightTs) {
		long elemLowerBound = leftTs + lowerBound;
		long elemUpperBound = leftTs + upperBound;

		return elemLowerBound <= rightTs && rightTs <= elemUpperBound;
	}

	private long maxCleanup(boolean isLeft, long watermark) {
		if (isLeft) {
			return (upperBound <= 0) ? watermark : watermark - upperBound;
		} else {
			return (lowerBound <= 0) ? watermark + lowerBound : watermark;
		}
	}

	private long calculateRemovalTime(boolean isLeft, long timestamp) {
		if (isLeft) {
			return (upperBound > 0) ? timestamp + upperBound : timestamp;
		} else {
			return (lowerBound < 0) ? timestamp - lowerBound : timestamp;
		}
	}

	private <T> void addToBuffer(
		MapState<Long, List<BufferEntry<T>>> buffer,
		T value,
		long ts
	) throws Exception {

		BufferEntry<T> elem = new BufferEntry<>(value, ts, false);

		List<BufferEntry<T>> elemsInBucket = buffer.get(ts);
		if (elemsInBucket == null) {
			elemsInBucket = new ArrayList<>();
		}

		elemsInBucket.add(elem);
		buffer.put(ts, elemsInBucket);
	}

	@Override
	public void onEventTime(InternalTimer<K, String> timer) throws Exception {
		logger.trace("onEventTime @ {}", timer.getTimestamp());

		long ts = timer.getTimestamp();
		String namespace = timer.getNamespace();

		switch (namespace) {
			case CLEANUP_NAMESPACE_LEFT: {
				long timestamp = maxCleanup(true, ts);
				logger.trace("Removing from left buffer @ {}", timestamp);
				removeFromBufferAt(leftBuffer, timestamp);
				break;
			}
			case CLEANUP_NAMESPACE_RIGHT: {
				long timestamp = maxCleanup(false, ts);
				logger.trace("Removing from right buffer @ {}", timestamp);
				removeFromBufferAt(rightBuffer, timestamp);
				break;
			}
			default:
				throw new RuntimeException("Invalid namespace " + namespace);
		}
	}

	private <T> void removeFromBufferAt(
		MapState<Long, List<BufferEntry<T>>> state,
		long timestamp
	) throws Exception {

		state.remove(timestamp);
	}

	@Override
	public void onProcessingTime(InternalTimer<K, String> timer) throws Exception {
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

	private static class BufferEntry<T> {
		T element;
		long timestamp;
		boolean hasBeenJoined;

		BufferEntry() {
		}

		BufferEntry(T element, long timestamp, boolean hasBeenJoined) {
			this.element = element;
			this.timestamp = timestamp;
			this.hasBeenJoined = hasBeenJoined;
		}
	}

	private static class BufferEntrySerializer<T> extends TypeSerializer<BufferEntry<T>> {

		private static final long serialVersionUID = -20197698803836236L;

		private final TypeSerializer<T> elementSerializer;

		private BufferEntrySerializer(TypeSerializer<T> elementSerializer) {
			this.elementSerializer = elementSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public TypeSerializer<BufferEntry<T>> duplicate() {
			return new BufferEntrySerializer<>(elementSerializer.duplicate());
		}

		@Override
		public BufferEntry<T> createInstance() {
			return new BufferEntry<>();
		}

		@Override
		public BufferEntry<T> copy(BufferEntry<T> from) {
			return new BufferEntry<>(from.element, from.timestamp, from.hasBeenJoined);
		}

		@Override
		public BufferEntry<T> copy(BufferEntry<T> from, BufferEntry<T> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return LongSerializer.INSTANCE.getLength()
				+ BooleanSerializer.INSTANCE.getLength()
				+ elementSerializer.getLength();
		}

		@Override
		public void serialize(BufferEntry<T> record, DataOutputView target) throws IOException {
			LongSerializer.INSTANCE.serialize(record.timestamp, target);
			BooleanSerializer.INSTANCE.serialize(record.hasBeenJoined, target);
			elementSerializer.serialize(record.element, target);
		}

		@Override
		public BufferEntry<T> deserialize(DataInputView source) throws IOException {
			long timestamp = LongSerializer.INSTANCE.deserialize(source);
			boolean hasBeenJoined = BooleanSerializer.INSTANCE.deserialize(source);
			T element = elementSerializer.deserialize(source);

			return new BufferEntry<>(element, timestamp, hasBeenJoined);
		}

		@Override
		public BufferEntry<T> deserialize(BufferEntry<T> reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			LongSerializer.INSTANCE.copy(source, target);
			BooleanSerializer.INSTANCE.copy(source, target);
			elementSerializer.copy(source, target);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			BufferEntrySerializer<?> that = (BufferEntrySerializer<?>) o;
			return Objects.equals(elementSerializer, that.elementSerializer);
		}

		@Override
		public int hashCode() {
			return Objects.hash(elementSerializer);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj.getClass().equals(BufferEntrySerializer.class);
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return new BufferSerializerConfigSnapshot(
				IntSerializer.INSTANCE.snapshotConfiguration(),
				LongSerializer.INSTANCE.snapshotConfiguration(),
				elementSerializer.snapshotConfiguration()
			);
		}

		@Override
		public CompatibilityResult<BufferEntry<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot.getVersion() == 1) {
				return CompatibilityResult.compatible();
			} else {
				return CompatibilityResult.requiresMigration();
			}
		}
	}

	public static class BufferSerializerConfigSnapshot extends TypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		private TypeSerializerConfigSnapshot longSnapshot;
		private TypeSerializerConfigSnapshot booleanSnapshot;
		private TypeSerializerConfigSnapshot userTypeSnapshot;

		public BufferSerializerConfigSnapshot() {
		}

		public BufferSerializerConfigSnapshot(
			TypeSerializerConfigSnapshot longSnapshot,
			TypeSerializerConfigSnapshot booleanSnapshot,
			TypeSerializerConfigSnapshot userTypeSnapshot
		) {

			this.longSnapshot = longSnapshot;
			this.booleanSnapshot = booleanSnapshot;
			this.userTypeSnapshot = userTypeSnapshot;
		}


		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			BufferSerializerConfigSnapshot that = (BufferSerializerConfigSnapshot) o;
			return Objects.equals(longSnapshot, that.longSnapshot) &&
				Objects.equals(booleanSnapshot, that.booleanSnapshot) &&
				Objects.equals(userTypeSnapshot, that.userTypeSnapshot);
		}

		@Override
		public int hashCode() {
			return Objects.hash(longSnapshot, booleanSnapshot, userTypeSnapshot);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}

	@VisibleForTesting
	MapState<Long, List<BufferEntry<T1>>> getLeftBuffer() {
		return leftBuffer;
	}

	@VisibleForTesting
	MapState<Long, List<BufferEntry<T2>>> getRightBuffer() {
		return rightBuffer;
	}
}
