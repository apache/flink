/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBufferFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Window operator for non-keyed windows.
 *
 * @see org.apache.flink.streaming.runtime.operators.windowing.WindowOperator
 *
 * @param <IN> The type of the incoming elements.
 * @param <ACC> The type of elements stored in the window buffers.
 * @param <OUT> The type of elements emitted by the {@code WindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class NonKeyedWindowOperator<IN, ACC, OUT, W extends Window>
		extends AbstractUdfStreamOperator<OUT, AllWindowFunction<ACC, OUT, W>>
		implements OneInputStreamOperator<IN, OUT>, Triggerable, InputTypeConfigurable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(WindowOperator.class);

	// ------------------------------------------------------------------------
	// Configuration values and stuff from the user
	// ------------------------------------------------------------------------

	private final WindowAssigner<? super IN, W> windowAssigner;

	private final Trigger<? super IN, ? super W> trigger;

	private final WindowBufferFactory<? super IN, ACC, ? extends WindowBuffer<IN, ACC>> windowBufferFactory;

	/**
	 * This is used to copy the incoming element because it can be put into several window
	 * buffers.
	 */
	private TypeSerializer<IN> inputSerializer;

	/**
	 * For serializing the window in checkpoints.
	 */
	private final TypeSerializer<W> windowSerializer;

	// ------------------------------------------------------------------------
	// State that is not checkpointed
	// ------------------------------------------------------------------------

	/**
	 * Processing time timers that are currently in-flight.
	 */
	private transient Map<Long, Set<Context>> processingTimeTimers;

	/**
	 * Current waiting watermark callbacks.
	 */
	private transient Map<Long, Set<Context>> watermarkTimers;

	/**
	 * This is given to the {@code WindowFunction} for emitting elements with a given timestamp.
	 */
	protected transient TimestampedCollector<OUT> timestampedCollector;

	/**
	 * To keep track of the current watermark so that we can immediately fire if a trigger
	 * registers an event time callback for a timestamp that lies in the past.
	 */
	protected transient long currentWatermark = -1L;

	// ------------------------------------------------------------------------
	// State that needs to be checkpointed
	// ------------------------------------------------------------------------

	/**
	 * The windows (panes) that are currently in-flight. Each pane has a {@code WindowBuffer}
	 * and a {@code TriggerContext} that stores the {@code Trigger} for that pane.
	 */
	protected transient Map<W, Context> windows;

	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public NonKeyedWindowOperator(WindowAssigner<? super IN, W> windowAssigner,
			TypeSerializer<W> windowSerializer,
			WindowBufferFactory<? super IN, ACC, ? extends WindowBuffer<IN, ACC>> windowBufferFactory,
			AllWindowFunction<ACC, OUT, W> windowFunction,
			Trigger<? super IN, ? super W> trigger) {

		super(windowFunction);

		this.windowAssigner = requireNonNull(windowAssigner);
		this.windowSerializer = windowSerializer;

		this.windowBufferFactory = requireNonNull(windowBufferFactory);
		this.trigger = requireNonNull(trigger);

		setChainingStrategy(ChainingStrategy.ALWAYS);
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
			watermarkTimers = new HashMap<>();
		}
		if (processingTimeTimers == null) {
			processingTimeTimers = new HashMap<>();
		}
		if (windows == null) {
			windows = new HashMap<>();
		}

		// re-register timers that this window context had set
		for (Context context: windows.values()) {
			if (context.processingTimeTimer > 0) {
				Set<Context> triggers = processingTimeTimers.get(context.processingTimeTimer);
				if (triggers == null) {
					getRuntimeContext().registerTimer(context.processingTimeTimer, NonKeyedWindowOperator.this);
					triggers = new HashSet<>();
					processingTimeTimers.put(context.processingTimeTimer, triggers);
				}
				triggers.add(context);
			}
			if (context.watermarkTimer > 0) {
				Set<Context> triggers = watermarkTimers.get(context.watermarkTimer);
				if (triggers == null) {
					triggers = new HashSet<>();
					watermarkTimers.put(context.watermarkTimer, triggers);
				}
				triggers.add(context);
			}

		}
	}

	@Override
	public final void dispose() {
		super.dispose();
		windows.clear();
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void processElement(StreamRecord<IN> element) throws Exception {
		Collection<W> elementWindows = windowAssigner.assignWindows(element.getValue(), element.getTimestamp());

		for (W window: elementWindows) {
			Context context = windows.get(window);
			if (context == null) {
				WindowBuffer<IN, ACC> windowBuffer = windowBufferFactory.create();
				context = new Context(window, windowBuffer);
				windows.put(window, context);
			}
			context.windowBuffer.storeElement(element);
			TriggerResult triggerResult = context.onElement(element);
			processTriggerResult(triggerResult, window);
		}
	}

	protected void emitWindow(Context context) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(context.window.maxTimestamp());

		if (context.windowBuffer.size() > 0) {
			userFunction.apply(
					context.window,
					context.windowBuffer.getUnpackedElements(),
					timestampedCollector);
		}
	}

	private void processTriggerResult(TriggerResult triggerResult, W window) throws Exception {
		if (!triggerResult.isFire() && !triggerResult.isPurge()) {
			// do nothing
			return;
		}
		Context context;

		if (triggerResult.isPurge()) {
			context = windows.remove(window);
		} else {
			context = windows.get(window);
		}
		if (context == null) {
			LOG.debug("Window {} already gone.", window);
			return;
		}

		if (triggerResult.isFire()) {
			emitWindow(context);
		}

		if (triggerResult.isPurge()) {
			context.clear();
		}
	}

	@Override
	public final void processWatermark(Watermark mark) throws Exception {
		List<Set<Context>> toTrigger = new ArrayList<>();

		Iterator<Map.Entry<Long, Set<Context>>> it = watermarkTimers.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, Set<Context>> triggers = it.next();
			if (triggers.getKey() <= mark.getTimestamp()) {
				toTrigger.add(triggers.getValue());
				it.remove();
			}
		}

		for (Set<Context> ctxs: toTrigger) {
			for (Context ctx: ctxs) {
				// double check the time. it can happen that the trigger registers a new timer,
				// in that case the entry is left in the watermarkTimers set for performance reasons.
				// We have to check here whether the entry in the set still reflects the
				// currently set timer in the Context.
				if (ctx.watermarkTimer <= mark.getTimestamp()) {
					TriggerResult triggerResult = ctx.onEventTime(ctx.watermarkTimer);
					processTriggerResult(triggerResult, ctx.window);
				}
			}
		}

		output.emitWatermark(mark);

		this.currentWatermark = mark.getTimestamp();
	}

	@Override
	public final void trigger(long time) throws Exception {
		List<Set<Context>> toTrigger = new ArrayList<>();

		Iterator<Map.Entry<Long, Set<Context>>> it = processingTimeTimers.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, Set<Context>> triggers = it.next();
			if (triggers.getKey() <= time) {
				toTrigger.add(triggers.getValue());
				it.remove();
			}
		}

		for (Set<Context> ctxs: toTrigger) {
			for (Context ctx: ctxs) {
				// double check the time. it can happen that the trigger registers a new timer,
				// in that case the entry is left in the processingTimeTimers set for
				// performance reasons. We have to check here whether the entry in the set still
				// reflects the currently set timer in the Context.
				if (ctx.processingTimeTimer <= time) {
					TriggerResult triggerResult = ctx.onProcessingTime(ctx.processingTimeTimer);
					processTriggerResult(triggerResult, ctx.window);
				}
			}
		}
	}

	/**
	 * The {@code Context} is responsible for keeping track of the state of one pane.
	 *
	 * <p>
	 * A pane is the bucket of elements that have the same key (assigned by the
	 * {@link org.apache.flink.api.java.functions.KeySelector}) and same {@link Window}. An element can
	 * be in multiple panes of it was assigned to multiple windows by the
	 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. These panes all
	 * have their own instance of the {@code Trigger}.
	 */
	protected class Context implements TriggerContext {
		protected W window;

		protected WindowBuffer<IN, ACC> windowBuffer;

		protected HashMap<String, Serializable> state;

		// use these to only allow one timer in flight at a time of each type
		// if the trigger registers another timer this value here will be overwritten,
		// the timer is not removed from the set of in-flight timers to improve performance.
		// When a trigger fires it is just checked against the last timer that was set.
		protected long watermarkTimer;
		protected long processingTimeTimer;

		public Context(
				W window,
				WindowBuffer<IN, ACC> windowBuffer) {
			this.window = window;
			this.windowBuffer = windowBuffer;
			state = new HashMap<>();

			this.watermarkTimer = -1;
			this.processingTimeTimer = -1;
		}

		@Override
		public long getCurrentWatermark() {
			return currentWatermark;
		}

		@SuppressWarnings("unchecked")
		protected Context(DataInputView in, ClassLoader userClassloader) throws Exception {
			this.window = windowSerializer.deserialize(in);
			this.watermarkTimer = in.readLong();
			this.processingTimeTimer = in.readLong();

			int stateSize = in.readInt();
			byte[] stateData = new byte[stateSize];
			in.read(stateData);
			state = InstantiationUtil.deserializeObject(stateData, userClassloader);

			this.windowBuffer = windowBufferFactory.restoreFromSnapshot(in);
		}

		protected void writeToState(AbstractStateBackend.CheckpointStateOutputView out) throws IOException {
			windowSerializer.serialize(window, out);
			out.writeLong(watermarkTimer);
			out.writeLong(processingTimeTimer);

			byte[] serializedState = InstantiationUtil.serializeObject(state);
			out.writeInt(serializedState.length);
			out.write(serializedState, 0, serializedState.length);

			windowBuffer.snapshot(out);
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			Class<S> stateType,
			S defaultState) {
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
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			TypeInformation<S> stateType,
			S defaultState) {

			requireNonNull(name, "The name of the state must not be null");
			requireNonNull(stateType, "The state type information must not be null");

			ValueStateDescriptor<S> stateDesc = new ValueStateDescriptor<>(
					name, stateType.createSerializer(getExecutionConfig()), defaultState);
			return getPartitionedState(stateDesc);
		}

		@Override
		@SuppressWarnings("rawtypes, unchecked")
		public <S extends State> S getPartitionedState(final StateDescriptor<S, ?> stateDescriptor) {
			if (!(stateDescriptor instanceof ValueStateDescriptor)) {
				throw new UnsupportedOperationException("NonKeyedWindowOperator Triggers only " +
					"support ValueState.");
			}
			@SuppressWarnings("unchecked")
			final ValueStateDescriptor<?> valueStateDescriptor = (ValueStateDescriptor<?>) stateDescriptor;
			ValueState valueState = new ValueState() {
				@Override
				public Object value() throws IOException {
					Object value = state.get(stateDescriptor.getName());
					if (value == null) {
						value = valueStateDescriptor.getDefaultValue();
						state.put(stateDescriptor.getName(), (Serializable) value);
					}
					return value;
				}

				@Override
				public void update(Object value) throws IOException {
					if (!(value instanceof Serializable)) {
						throw new UnsupportedOperationException(
							"Value state of NonKeyedWindowOperator must be serializable.");
					}
					state.put(stateDescriptor.getName(), (Serializable) value);
				}

				@Override
				public void clear() {
					state.remove(stateDescriptor.getName());
				}
			};
			return (S) valueState;
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			if (this.processingTimeTimer == time) {
				// we already have set a trigger for that time
				return;
			}
			Set<Context> triggers = processingTimeTimers.get(time);
			if (triggers == null) {
				getRuntimeContext().registerTimer(time, NonKeyedWindowOperator.this);
				triggers = new HashSet<>();
				processingTimeTimers.put(time, triggers);
			}
			this.processingTimeTimer = time;
			triggers.add(this);
		}

		@Override
		public void registerEventTimeTimer(long time) {
			if (watermarkTimer == time) {
				// we already have set a trigger for that time
				return;
			}
			Set<Context> triggers = watermarkTimers.get(time);
			if (triggers == null) {
				triggers = new HashSet<>();
				watermarkTimers.put(time, triggers);
			}
			this.watermarkTimer = time;
			triggers.add(this);
		}

		@Override
		public void deleteProcessingTimeTimer(long time) {
			Set<Context> triggers = processingTimeTimers.get(time);
			if (triggers != null) {
				triggers.remove(this);
			}
		}

		@Override
		public void deleteEventTimeTimer(long time) {
			Set<Context> triggers = watermarkTimers.get(time);
			if (triggers != null) {
				triggers.remove(this);
			}

		}

		public TriggerResult onElement(StreamRecord<IN> element) throws Exception {
			TriggerResult onElementResult = trigger.onElement(element.getValue(), element.getTimestamp(), window, this);
			if (watermarkTimer > 0 && watermarkTimer <= currentWatermark) {
				// fire now and don't wait for the next watermark update
				TriggerResult onEventTimeResult = onEventTime(watermarkTimer);
				return TriggerResult.merge(onElementResult, onEventTimeResult);
			} else {
				return onElementResult;
			}
		}

		public TriggerResult onProcessingTime(long time) throws Exception {
			if (time == processingTimeTimer) {
				processingTimeTimer = -1;
				return trigger.onProcessingTime(time, window, this);
			} else {
				return TriggerResult.CONTINUE;
			}
		}

		public TriggerResult onEventTime(long time) throws Exception {
			if (time == watermarkTimer) {
				watermarkTimer = -1;
				TriggerResult firstTriggerResult = trigger.onEventTime(time, window, this);

				if (watermarkTimer > 0 && watermarkTimer <= currentWatermark) {
					// fire now and don't wait for the next watermark update
					TriggerResult secondTriggerResult = onEventTime(watermarkTimer);
					return TriggerResult.merge(firstTriggerResult, secondTriggerResult);
				} else {
					return firstTriggerResult;
				}

			} else {
				return TriggerResult.CONTINUE;
			}
		}

		public void clear() throws Exception {
			trigger.clear(window, this);
		}
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		// we write the panes with the key/value maps into the stream
		AbstractStateBackend.CheckpointStateOutputView out = getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

		int numWindows = windows.size();
		out.writeInt(numWindows);
		for (Context context: windows.values()) {
			context.writeToState(out);
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

		int numWindows = in.readInt();
		this.windows = new HashMap<>(numWindows);
		this.processingTimeTimers = new HashMap<>();
		this.watermarkTimers = new HashMap<>();

		for (int j = 0; j < numWindows; j++) {
			Context context = new Context(in, userClassloader);
			windows.put(context.window, context);
		}
	}


	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public Trigger<? super IN, ? super W> getTrigger() {
		return trigger;
	}

	@VisibleForTesting
	public WindowAssigner<? super IN, W> getWindowAssigner() {
		return windowAssigner;
	}

	@VisibleForTesting
	public WindowBufferFactory<? super IN, ACC, ? extends WindowBuffer<IN, ACC>> getWindowBufferFactory() {
		return windowBufferFactory;
	}
}
