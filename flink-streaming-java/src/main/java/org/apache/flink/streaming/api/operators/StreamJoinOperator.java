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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

import static java.util.Objects.requireNonNull;


public class StreamJoinOperator<K, IN1, IN2, OUT>
		extends AbstractUdfStreamOperator<OUT, JoinFunction<IN1, IN2, OUT>>
		implements TwoInputStreamOperator<IN1, IN2, OUT> , Triggerable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(StreamJoinOperator.class);

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	protected final WindowAssigner<Object, TimeWindow> windowAssigner1;
	protected final WindowAssigner<Object, TimeWindow> windowAssigner2;
	// windowSize1 >= windowSize2
	protected final long windowSize1;
	protected final long windowSize2;

	protected final KeySelector<IN1, K> keySelector1;
	protected final KeySelector<IN2, K> keySelector2;

	protected final StateDescriptor<? extends ListState<CoGroupedStreams.TaggedUnion<IN1, IN2>>, ?> windowStateDescriptor;

	protected final Trigger<Object, TimeWindow> trigger;


	/**
	 * If this is true. The current processing time is set as the timestamp of incoming elements.
	 * This for use with a {@link org.apache.flink.streaming.api.windowing.evictors.TimeEvictor}
	 * if eviction should happen based on processing time.
	 */
	protected boolean setProcessingTime = false;

	/**
	 * This is used to copy the incoming element because it can be put into several window
	 * buffers.
	 */
	protected TypeSerializer<IN1> inputSerializer1;
	protected TypeSerializer<IN2> inputSerializer2;

	/**
	 * For serializing the key in checkpoints.
	 */
	protected final TypeSerializer<K> keySerializer;

	/**
	 * For serializing the window in checkpoints.
	 */
	protected final TypeSerializer<TimeWindow> windowSerializer1;
	protected final TypeSerializer<TimeWindow> windowSerializer2;

	// ------------------------------------------------------------------------
	// State that is not checkpointed
	// ------------------------------------------------------------------------

	/**
	 * This is given to the {@code WindowFunction} for emitting elements with a given timestamp.
	 */
	protected transient TimestampedCollector<OUT> timestampedCollector;

	protected transient long currentWatermark1 = -1L;
	protected transient long currentWatermark2 = -1L;
	protected transient long currentWatermark = -1L;
	protected transient Context context = new Context(null, null);

	// ------------------------------------------------------------------------
	// State that needs to be checkpointed
	// ------------------------------------------------------------------------
	/**
	 * Processing time timers that are currently in-flight.
	 */
	protected transient Set<Timer<K, TimeWindow>> processingTimeTimers;
	protected transient PriorityQueue<Timer<K, TimeWindow>> processingTimeTimersQueue;

	/**
	 * Current waiting watermark callbacks.
	 */
	protected transient Set<Timer<K, TimeWindow>> watermarkTimers;
	protected transient PriorityQueue<Timer<K, TimeWindow>> watermarkTimersQueue;


	public StreamJoinOperator(JoinFunction<IN1, IN2, OUT> userFunction,
					KeySelector<IN1, K> keySelector1,
					KeySelector<IN2, K> keySelector2,
					TypeSerializer<K> keySerializer,
					WindowAssigner<Object, TimeWindow> windowAssigner1,
					TypeSerializer<TimeWindow> windowSerializer1,
					long windowSize1,
					WindowAssigner<Object, TimeWindow> windowAssigner2,
					TypeSerializer<TimeWindow> windowSerializer2,
					long windowSize2,
					StateDescriptor<? extends ListState<CoGroupedStreams.TaggedUnion<IN1, IN2>>, ?> windowStateDescriptor,
					TypeSerializer<IN1> inputSerializer1,
					TypeSerializer<IN2> inputSerializer2,
					Trigger<Object, TimeWindow> trigger) {
		super(userFunction);
		this.keySelector1 = requireNonNull(keySelector1);
		this.keySelector2 = requireNonNull(keySelector2);
		this.keySerializer = requireNonNull(keySerializer);

		this.windowAssigner1 = requireNonNull(windowAssigner1);
		this.windowSerializer1 = requireNonNull(windowSerializer1);
		this.windowSize1 = windowSize1;
		this.windowAssigner2 = requireNonNull(windowAssigner2);
		this.windowSerializer2 = requireNonNull(windowSerializer2);
		this.windowSize2 = windowSize2;

		this.windowStateDescriptor = requireNonNull(windowStateDescriptor);

		this.inputSerializer1 = requireNonNull(inputSerializer1);
		this.inputSerializer2 = requireNonNull(inputSerializer2);
		this.trigger = requireNonNull(trigger);
	}

	@Override
	public void open() throws Exception {
		super.open();
		timestampedCollector = new TimestampedCollector<>(output);

		if (null == inputSerializer1 || null == inputSerializer2) {
			throw new IllegalStateException("Input serializer was not set.");
		}

		// these could already be initialized from restoreState()
		if (watermarkTimers == null) {
			watermarkTimers = new HashSet<>();
			watermarkTimersQueue = new PriorityQueue<>(100);
		}

		if (processingTimeTimers == null) {
			processingTimeTimers = new HashSet<>();
			processingTimeTimersQueue = new PriorityQueue<>(100);
		}

		context = new Context(null, null);
	}

	/**
	 * @param element record of stream1
	 * @throws Exception
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void processElement1(StreamRecord<IN1> element) throws Exception {
		if (setProcessingTime) {
			element.replace(element.getValue(), System.currentTimeMillis());
		}

		Collection<TimeWindow> elementWindows = windowAssigner1.assignWindows(element.getValue(), element.getTimestamp());

		K key = (K) getStateBackend().getCurrentKey();

		for (TimeWindow window: elementWindows) {

			ListState<CoGroupedStreams.TaggedUnion<IN1, IN2>> windowState = getPartitionedState(window, windowSerializer1,
					windowStateDescriptor);
			context.key = key;
			context.window = window;
			CoGroupedStreams.TaggedUnion<IN1, IN2> unionElement = CoGroupedStreams.TaggedUnion.one(element.getValue());
			windowState.add(unionElement);
			TriggerResult triggerResult = context.onElement(unionElement, element.getTimestamp());
			processTriggerResult(triggerResult, key, window);

		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		if (setProcessingTime) {
			element.replace(element.getValue(), System.currentTimeMillis());
		}

		Collection<TimeWindow> elementWindows2 = windowAssigner2.assignWindows(element.getValue(), element.getTimestamp());
		Collection<TimeWindow> elementWindows1 = new ArrayList<>(elementWindows2.size());
		// Convert windows of stream2 to corresponding windows of stream1
		for(TimeWindow window : elementWindows2){
			elementWindows1.add(new TimeWindow(window.getEnd() - windowSize1, window.getEnd()));
		}
		K key = (K) getStateBackend().getCurrentKey();

		for (TimeWindow window: elementWindows1) {

			ListState<CoGroupedStreams.TaggedUnion<IN1, IN2>> windowState = getPartitionedState(window, windowSerializer1,
					windowStateDescriptor);
			context.key = key;
			context.window = window;
			CoGroupedStreams.TaggedUnion<IN1, IN2> unionElement = CoGroupedStreams.TaggedUnion.two(element.getValue());
			windowState.add(unionElement);

			TriggerResult triggerResult = context.onElement(unionElement, element.getTimestamp());
			processTriggerResult(triggerResult, key, window);
		}
	}

	protected void processTriggerResult(TriggerResult triggerResult, K key, TimeWindow window) throws Exception {
		if (!triggerResult.isFire() && !triggerResult.isPurge()) {
			// do nothing
			return;
		}

		if (triggerResult.isFire()) {
			timestampedCollector.setTimestamp(window.maxTimestamp());

			ListState<CoGroupedStreams.TaggedUnion<IN1, IN2>> windowState = getPartitionedState(window, windowSerializer1,
					windowStateDescriptor);

			Iterable<CoGroupedStreams.TaggedUnion<IN1, IN2>> contents = windowState.get();

			List<IN1> oneValues = new ArrayList<>();
			List<IN2> twoValues = new ArrayList<>();

			for (CoGroupedStreams.TaggedUnion<IN1, IN2> val: contents) {
				if (val.isOne()) {
					oneValues.add(val.getOne());
				} else {
					twoValues.add(val.getTwo());
				}
			}
			for (IN1 val1: oneValues) {
				for (IN2 val2: twoValues) {
					timestampedCollector.collect(userFunction.join(val1, val2));
				}
			}
			if (triggerResult.isPurge()) {
				windowState.clear();
				context.clear();
			}
		} else if (triggerResult.isPurge()) {
			ListState<CoGroupedStreams.TaggedUnion<IN1, IN2>> windowState = getPartitionedState(window, windowSerializer1,
					windowStateDescriptor);
			windowState.clear();
			context.clear();
		}
	}

	/**
	 * Process join operator on element during [currentWaterMark, watermark)
	 * @param watermark
	 * @throws Exception
	 */
	private void processWatermark(long watermark) throws Exception{
		boolean fire;

		do {
			Timer<K, TimeWindow> timer = watermarkTimersQueue.peek();
			if (timer != null && timer.timestamp <= watermark) {
				fire = true;

				watermarkTimers.remove(timer);
				watermarkTimersQueue.remove();

				context.key = timer.key;
				context.window = timer.window;
				setKeyContext(timer.key);
				TriggerResult triggerResult = context.onEventTime(timer.timestamp);
				processTriggerResult(triggerResult, context.key, context.window);
			} else {
				fire = false;
			}
		} while (fire);

		this.currentWatermark = watermark;
	}

	@Override
	public void processWatermark1(Watermark mark) throws Exception {
		long watermark = Math.min(mark.getTimestamp(), currentWatermark2);
		// process elements [currentWatermark, watermark)
		processWatermark(watermark);

		output.emitWatermark(mark);
		this.currentWatermark = watermark;
		this.currentWatermark1 = mark.getTimestamp();
	}

	@Override
	public void processWatermark2(Watermark mark) throws Exception {
		long watermark = Math.min(mark.getTimestamp(), currentWatermark1);
		// process elements [currentWatermark, watermark)
		processWatermark(watermark);

		output.emitWatermark(mark);
		this.currentWatermark = watermark;
		this.currentWatermark2 = mark.getTimestamp();
	}


	@Override
	public final void trigger(long time) throws Exception {
		boolean fire;

		do {
			Timer<K, TimeWindow> timer = processingTimeTimersQueue.peek();
			if (timer != null && timer.timestamp <= time) {
				fire = true;

				processingTimeTimers.remove(timer);
				processingTimeTimersQueue.remove();

				context.key = timer.key;
				context.window = timer.window;
				setKeyContext(timer.key);
				TriggerResult triggerResult = context.onProcessingTime(timer.timestamp);
				processTriggerResult(triggerResult, context.key, context.window);
			} else {
				fire = false;
			}
		} while (fire);

		// Also check any watermark timers. We might have some in here since
		// Context.registerEventTimeTimer sets a trigger if an event-time trigger is registered
		// that is already behind the watermark.
		processWatermark(currentWatermark);
	}

	/**
	 * When this flag is enabled the current processing time is set as the timestamp of elements
	 * upon arrival. This must be used, for example, when using the
	 * {@link org.apache.flink.streaming.api.windowing.evictors.TimeEvictor} with processing
	 * time semantics.
	 */
	public StreamJoinOperator<K, IN1, IN2, OUT> enableSetProcessingTime(boolean setProcessingTime) {
		this.setProcessingTime = setProcessingTime;
		return this;
	}


	/**
	 * {@code Context} is a utility for handling {@code Trigger} invocations. It can be reused
	 * by setting the {@code key} and {@code window} fields. No internal state must be kept in
	 * the {@code Context}
	 */
	protected class Context implements Trigger.TriggerContext {
		protected K key;
		protected TimeWindow window;

		public Context(K key, TimeWindow window) {
			this.key = key;
			this.window = window;
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name, Class<S> stateType, S defaultState) {
			requireNonNull(stateType, "The state type class must not be null");

			TypeInformation<S> typeInfo;
			try {
				typeInfo = TypeExtractor.getForClass(stateType);
			}
			catch (Exception e) {
				throw new RuntimeException("Cannot analyze type '" + stateType.getName() +
						"' from the class alone, due to generic type parameters. " +
						"Please specify the TypeInformation directly.", e);
			}

			return getKeyValueState(name, typeInfo, defaultState);
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name, TypeInformation<S> stateType, S defaultState) {

			requireNonNull(name, "The name of the state must not be null");
			requireNonNull(stateType, "The state type information must not be null");

			ValueStateDescriptor<S> stateDesc = new ValueStateDescriptor<>(name, stateType.createSerializer(getExecutionConfig()), defaultState);
			return getPartitionedState(stateDesc);
		}

		@SuppressWarnings("unchecked")
		public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			try {
				return StreamJoinOperator.this.getPartitionedState(window, windowSerializer1, stateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve state", e);
			}
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			Timer<K, TimeWindow> timer = new Timer<>(time, key, window);
			if (processingTimeTimers.add(timer)) {
				processingTimeTimersQueue.add(timer);
				getRuntimeContext().registerTimer(time, StreamJoinOperator.this);
			}
		}

		@Override
		public void registerEventTimeTimer(long time) {
			Timer<K, TimeWindow> timer = new Timer<>(time, key, window);
			if (watermarkTimers.add(timer)) {
				watermarkTimersQueue.add(timer);
			}

			if (time <= currentWatermark) {
				// immediately schedule a trigger, so that we don't wait for the next
				// watermark update to fire the watermark trigger
				getRuntimeContext().registerTimer(time, StreamJoinOperator.this);
			}
		}

		@Override
		public void deleteProcessingTimeTimer(long time) {
			Timer<K, TimeWindow> timer = new Timer<>(time, key, window);
			if (processingTimeTimers.remove(timer)) {
				processingTimeTimersQueue.remove(timer);
			}
		}

		@Override
		public void deleteEventTimeTimer(long time) {
			Timer<K, TimeWindow> timer = new Timer<>(time, key, window);
			if (watermarkTimers.remove(timer)) {
				watermarkTimersQueue.remove(timer);
			}

		}

		public TriggerResult onElement(CoGroupedStreams.TaggedUnion<IN1, IN2> element, long timestamp) throws Exception {
			return trigger.onElement(element, timestamp, window, this);
		}

		public TriggerResult onProcessingTime(long time) throws Exception {
			return trigger.onProcessingTime(time, window, this);
		}

		public TriggerResult onEventTime(long time) throws Exception {
			return trigger.onEventTime(time, window, this);
		}

		public void clear() throws Exception {
			trigger.clear(window, this);
		}

		@Override
		public String toString() {
			return "Context{" +
					"key=" + key +
					", window=" + window +
					'}';
		}
	}

	/**
	 * Internal class for keeping track of in-flight timers.
	 */
	protected static class Timer<K, W extends Window> implements Comparable<Timer<K, W>> {
		protected long timestamp;
		protected K key;
		protected W window;

		public Timer(long timestamp, K key, W window) {
			this.timestamp = timestamp;
			this.key = key;
			this.window = window;
		}

		@Override
		public int compareTo(Timer<K, W> o) {
			return Long.compare(this.timestamp, o.timestamp);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()){
				return false;
			}

			Timer<?, ?> timer = (Timer<?, ?>) o;

			return timestamp == timer.timestamp
					&& key.equals(timer.key)
					&& window.equals(timer.window);

		}

		@Override
		public int hashCode() {
			int result = (int) (timestamp ^ (timestamp >>> 32));
			result = 31 * result + key.hashCode();
			result = 31 * result + window.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "Timer{" +
					"timestamp=" + timestamp +
					", key=" + key +
					", window=" + window +
					'}';
		}
	}

	// ------------------------------------------------------------------------
	//  checkpointing and recovery
	// ------------------------------------------------------------------------

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		AbstractStateBackend.CheckpointStateOutputView out =
				getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

		out.writeInt(watermarkTimersQueue.size());
		for (Timer<K, TimeWindow> timer : watermarkTimersQueue) {
			keySerializer.serialize(timer.key, out);
			windowSerializer1.serialize(timer.window, out);
			out.writeLong(timer.timestamp);
		}

		out.writeInt(processingTimeTimers.size());
		for (Timer<K, TimeWindow> timer : processingTimeTimersQueue) {
			keySerializer.serialize(timer.key, out);
			windowSerializer1.serialize(timer.window, out);
			out.writeLong(timer.timestamp);
		}

		taskState.setOperatorState(out.closeAndGetHandle());

		return taskState;
	}

	@Override
	public void restoreState(StreamTaskState taskState, long recoveryTimestamp) throws Exception {
		super.restoreState(taskState, recoveryTimestamp);

		final ClassLoader userClassloader = getUserCodeClassloader();

		@SuppressWarnings("unchecked")
		StateHandle<DataInputView> inputState = (StateHandle<DataInputView>) taskState.getOperatorState();
		DataInputView in = inputState.getState(userClassloader);

		int numWatermarkTimers = in.readInt();
		watermarkTimers = new HashSet<>(numWatermarkTimers);
		watermarkTimersQueue = new PriorityQueue<>(Math.max(numWatermarkTimers, 1));
		for (int i = 0; i < numWatermarkTimers; i++) {
			K key = keySerializer.deserialize(in);
			TimeWindow window = windowSerializer1.deserialize(in);
			long timestamp = in.readLong();
			Timer<K, TimeWindow> timer = new Timer<>(timestamp, key, window);
			watermarkTimers.add(timer);
			watermarkTimersQueue.add(timer);
		}

		int numProcessingTimeTimers = in.readInt();
		processingTimeTimers = new HashSet<>(numProcessingTimeTimers);
		processingTimeTimersQueue = new PriorityQueue<>(Math.max(numProcessingTimeTimers, 1));
		for (int i = 0; i < numProcessingTimeTimers; i++) {
			K key = keySerializer.deserialize(in);
			TimeWindow window = windowSerializer1.deserialize(in);
			long timestamp = in.readLong();
			Timer<K, TimeWindow> timer = new Timer<>(timestamp, key, window);
			processingTimeTimers.add(timer);
			processingTimeTimersQueue.add(timer);
		}
	}

}
