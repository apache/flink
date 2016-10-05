/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.windowing.triggers.triggerdsl;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link Trigger} used to run the user specified {@link DslTrigger}. Dsl triggers
 * are allowed to be composed into tree-like structures, e.g.:
 *
 * <pre>{@code
 *
 *         EventTime.<TimeWindow>afterEndOfWindow()
 *             .withEarlyTrigger(ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(100)))
 *             .withLateTrigger(ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(200)))
 *             .discarding()
 * }</pre>
 *
 * Given this specification, the runner will build a tree of {@link DslTriggerInvokable TriggerInvokables},
 * each responsible for executing one of the dsl trigger specification in the tree. After this is done,
 * the runner is used as a classic trigger and makes sure that the trigger calls are propagated correctly
 * through the dsl trigger tree.
 *
 * @param <T> The type of elements on which this {@code Trigger} works.
 * @param <W> The type of {@link Window Windows} on which this {@code Trigger} can operate.
 */
public class DslTriggerRunner<T, W extends Window> extends Trigger<T, W> {

	/** The root dsl trigger. */
	private final DslTrigger<T, W> trigger;

	/** The invokable corresponding to the root dsl trigger. */
	private DslTriggerInvokable<T, W> invokable;

	public DslTriggerRunner(DslTrigger<T, W> trigger) {
		Preconditions.checkNotNull(trigger, "The trigger cannot be null");
		this.trigger = trigger.isRepeated().once() ? Repeat.Once(trigger) : trigger;
		this.trigger.setIsDiscarding(trigger.isDiscarding());
	}

	public Trigger<T, W> getTrigger() {
		return this;
	}

	/**
	 * Creates the tree of {@link DslTriggerInvokable TriggerInvokables} corresponding to the
	 * dsl trigger specification. In addition, it makes sure that the specified trigger
	 * combinations are valid. This method is called by the
	 * {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator}.
	 * @param windowSerializer The serializer of the windows used.
	 * @param allowedLateness The specified allowed lateness.
	 */
	public void createTriggerTree(TypeSerializer<W> windowSerializer, long allowedLateness) {
		Map<Integer, DslTriggerInvokable<T, W>> triggerIdToInvokableMapping = new HashMap<>();
		this.createTriggerInvokableTree(trigger, null, new ArrayList<DslTrigger<T, W>>(),
			triggerIdToInvokableMapping, windowSerializer, allowedLateness);
		this.invokable = triggerIdToInvokableMapping.get(0);
	}

	/**
	 * Recursively creates a {@link DslTriggerInvokable} for each {@link Trigger trigger} in the
	 * (possibly composite) trigger used by the window operator and creates a mapping
	 * between the created invokable and its unique id in the trigger tree.
	 */
	private void createTriggerInvokableTree(DslTrigger<T, W> trigger,
											DslTriggerInvokable<T, W> parentInvokable,
											List<DslTrigger<T, W>> flattenedTriggerList,
											Map<Integer, DslTriggerInvokable<T, W>> triggerIdToInvokableMapping,
											TypeSerializer<W> windowSerializer,
											long allowedLateness) {

		DslTrigger<T, W> actualTrigger = trigger.translate(windowSerializer, allowedLateness);
		flattenedTriggerList.add(actualTrigger);
		int actualTriggerId = flattenedTriggerList.lastIndexOf(actualTrigger);

		DslTriggerInvokable<T, W> actualTriggerInvokable = createInvokable(actualTriggerId, parentInvokable, actualTrigger);
		triggerIdToInvokableMapping.put(actualTriggerId, actualTriggerInvokable);

		List<DslTrigger<T, W>> childTriggers = actualTrigger.getChildTriggers();
		if (childTriggers == null || childTriggers.isEmpty()) {
			return;
		}

		Repeated parentIsRepeated = actualTriggerInvokable.isRepeated();
		for(DslTrigger<T, W> childTrigger : childTriggers) {

			// set the isRepeated flag
			if (parentIsRepeated.isUndefined() && childTrigger.isRepeated().once()) {
				// we cannot explicitly set the ONCE, so if the repeated flag of the parent is UNDEFINED,
				// e.g. in the after-end-of-window triggers, and we are the first ONCE in the tree, then
				// state it explicitly by wrapping the trigger in a Repeat.Once().
				childTrigger = Repeat.Once(childTrigger);
			}

			createTriggerInvokableTree(childTrigger, actualTriggerInvokable, flattenedTriggerList,
				triggerIdToInvokableMapping, windowSerializer, allowedLateness);

			// this works because we cannot have in the same branch of the trigger tree the same
			// object (dslTrigger) twice. There is no way to say "put myself in my subtree".

			int childId = flattenedTriggerList.lastIndexOf(childTrigger);
			DslTriggerInvokable<T, W> childInvokable = triggerIdToInvokableMapping.get(childId);
			actualTriggerInvokable.addChild(childInvokable);
		}
	}

	private DslTriggerInvokable<T, W> createInvokable(int id, DslTriggerInvokable<T, W> parent, DslTrigger<T, W> child) {
		DslTriggerInvokable<T, W> invokable = new DslTriggerInvokable<>(id, parent, child);

		List<StateDescriptor<State, ?>> descriptors = child.getStateDescriptors();
		if (descriptors != null) {
			for (StateDescriptor<State, ?> descriptor: descriptors) {
				invokable.translateStateDescriptor(descriptor);
			}
		}
		return invokable;
	}

	@Override
	public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
		invokable.setContext(ctx);
		boolean triggerResult = invokable.invokeOnElement(element, timestamp, window);
		if (!triggerResult) {
			return TriggerResult.CONTINUE;
		}
		return trigger.isDiscarding() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.FIRE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
		return shouldFireOnTimer(time, window, ctx, false);
	}

	@Override
	public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
		return shouldFireOnTimer(time, window, ctx, true);
	}

	private TriggerResult shouldFireOnTimer(long time, W window, TriggerContext ctx, boolean isEventTime) throws Exception {
		invokable.setContext(ctx);
		boolean triggerResult = invokable.invokeShouldFire(time, isEventTime, window);
		if (!triggerResult) {
			return TriggerResult.CONTINUE;
		}
		return trigger.isDiscarding() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.FIRE;
	}

	@Override
	public void onFire(W window, TriggerContext ctx) throws Exception {
		invokable.setContext(ctx);
		invokable.invokeOnFire(window);
	}

	@Override
	public void clear(W window, TriggerContext ctx) throws Exception {
		invokable.setContext(ctx);
		invokable.invokeClear(window);
	}

	@Override
	public boolean canMerge() {
		return trigger.canMerge();
	}

	@Override
	public TriggerResult onMerge(W window, OnMergeContext ctx) throws Exception {
		invokable.setContext(ctx);
		boolean triggerResult = invokable.invokeOnMerge(window);
		if (!triggerResult) {
			return TriggerResult.CONTINUE;
		}
		return trigger.isDiscarding() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.FIRE;
	}

	@Override
	public String toString() {
		return trigger.toString() + ((trigger.isDiscarding() ? ".discarding()" : ".accumulating()"));
	}
}
