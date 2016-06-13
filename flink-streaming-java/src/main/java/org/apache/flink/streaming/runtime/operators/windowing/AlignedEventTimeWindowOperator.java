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

package org.apache.flink.streaming.runtime.operators.windowing;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateIterator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Special-purpose window operator that behaves like {@link WindowOperator} with an
 * {@link org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger} but with triggering
 * behaviour aligned for all keys. This means that there is only one window timer per window,
 * not per key and window. This also means that all windows always fire in lockstep.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 */
@Internal
public class AlignedEventTimeWindowOperator<K, IN, ACC, OUT>
	extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, TimeWindow>>
	implements OneInputStreamOperator<IN, OUT>, InputTypeConfigurable {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	private final WindowAssigner<? super IN, TimeWindow> windowAssigner;

	private final StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor;

	/**
	 * This is used to copy the incoming element because it can be put into several window
	 * buffers.
	 */
	private TypeSerializer<IN> inputSerializer;

	/**
	 * For serializing the window in checkpoints.
	 */
	private final TypeSerializer<TimeWindow> windowSerializer;

	// ------------------------------------------------------------------------
	// State that is not checkpointed
	// ------------------------------------------------------------------------

	/**
	 * This is given to the {@code InternalWindowFunction} for emitting elements with a given timestamp.
	 */
	private transient TimestampedCollector<OUT> timestampedCollector;

	/**
	 * To keep track of the current watermark so that we can immediately fire if a trigger
	 * registers an event time callback for a timestamp that lies in the past.
	 */
	private transient long currentWatermark = -1L;

	// ------------------------------------------------------------------------
	// State that needs to be checkpointed
	// ------------------------------------------------------------------------

	/**
	 * Current waiting watermark callbacks.
	 */
	private transient Set<TimeWindow> watermarkTimers;
	private transient PriorityQueue<TimeWindow> watermarkTimersQueue;

	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public AlignedEventTimeWindowOperator(WindowAssigner<? super IN, TimeWindow> windowAssigner,
		TypeSerializer<TimeWindow> windowSerializer,
		StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor,
		InternalWindowFunction<ACC, OUT, K, TimeWindow> windowFunction) {

		super(windowFunction);

		this.windowAssigner = requireNonNull(windowAssigner);
		this.windowSerializer = windowSerializer;

		this.windowStateDescriptor = windowStateDescriptor;
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		currentWatermark = -1;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		inputSerializer = (TypeSerializer<IN>) type.createSerializer(executionConfig);
	}

	@Override
	public final void open() throws Exception {
		super.open();

		timestampedCollector = new TimestampedCollector<>(output);

		if (inputSerializer == null) {
			throw new IllegalStateException("Input serializer was not set.");
		}

		// these could already be initialized from restoreState()
		if (watermarkTimers == null) {
			watermarkTimers = new HashSet<>();
			watermarkTimersQueue = new PriorityQueue<>(100, new Comparator<TimeWindow>() {
				@Override
				public int compare(TimeWindow o1, TimeWindow o2) {
					return Long.compare(o1.maxTimestamp(), o2.maxTimestamp());
				}
			});
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void processElement(StreamRecord<IN> element) throws Exception {
		Collection<TimeWindow> elementWindows = windowAssigner.assignWindows(element.getValue(), element.getTimestamp());

		for (TimeWindow window: elementWindows) {
			AppendingState<IN, ACC> windowState = getPartitionedState(window, windowSerializer, windowStateDescriptor);
			windowState.add(element.getValue());

			if (window.maxTimestamp() <= currentWatermark) {
				// late element, we emit just like EventTimeTrigger does
				timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());

				ACC contents = windowState.get();
				userFunction.apply((K) getStateBackend().getCurrentKey(), window, contents, timestampedCollector);
				windowState.clear();

			} else {
				if (watermarkTimers.add(window)) {
					watermarkTimersQueue.add(window);
				}
			}
		}
	}

	private void fireForAllKeys(TimeWindow window) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());

		StateIterator<K, ? extends AppendingState<IN, ACC>> stateIterator = getStateBackend().getPartitionedStateForAllKeys(
				window,
				windowSerializer,
				windowStateDescriptor);

		while (stateIterator.advance()) {
			AppendingState<IN, ACC> windowState = stateIterator.state();
			ACC contents = windowState.get();
			
			// set key in case user function accesses state
			getStateBackend().setCurrentKey(stateIterator.key());
			userFunction.apply(stateIterator.key(), window, contents, timestampedCollector);

			stateIterator.delete();
		}
	}

	@Override
	public final void processWatermark(Watermark mark) throws Exception {

		boolean fire;

		do {
			TimeWindow timer = watermarkTimersQueue.peek();
			if (timer != null && timer.maxTimestamp() <= mark.getTimestamp()) {
				fire = true;

				watermarkTimers.remove(timer);
				watermarkTimersQueue.remove();

				fireForAllKeys(timer);
			} else {
				fire = false;
			}
		} while (fire);

		output.emitWatermark(mark);

		this.currentWatermark = mark.getTimestamp();
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		AbstractStateBackend.CheckpointStateOutputView out =
			getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

		out.writeInt(watermarkTimersQueue.size());
		for (TimeWindow timer : watermarkTimersQueue) {
			windowSerializer.serialize(timer, out);
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
		watermarkTimersQueue = new PriorityQueue<>(Math.max(numWatermarkTimers, 1),
				new Comparator<TimeWindow>() {
					@Override
					public int compare(TimeWindow o1, TimeWindow o2) {
						return Long.compare(o1.maxTimestamp(), o2.maxTimestamp());
					}
				});

		for (int i = 0; i < numWatermarkTimers; i++) {
			TimeWindow timer = windowSerializer.deserialize(in);
			watermarkTimers.add(timer);
			watermarkTimersQueue.add(timer);
		}
	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public WindowAssigner<? super IN, TimeWindow> getWindowAssigner() {
		return windowAssigner;
	}

	@VisibleForTesting
	public StateDescriptor<? extends AppendingState<IN, ACC>, ?> getStateDescriptor() {
		return windowStateDescriptor;
	}
}
