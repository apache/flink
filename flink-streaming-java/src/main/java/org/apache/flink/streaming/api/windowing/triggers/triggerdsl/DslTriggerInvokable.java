/*
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
package org.apache.flink.streaming.api.windowing.triggers.triggerdsl;

import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DslTriggerInvokable<T, W extends Window> implements Serializable {

	private static final long serialVersionUID = 1L;

	private final int triggerId;
	private final DslTriggerInvokable<T, W> parent;
	private final List<DslTriggerInvokable<T, W>> children = new ArrayList<>();

	private final DslTrigger<T, W> trigger;
	private final DslTriggerContext context;

	private final Map<String, StateDescriptor<? extends State, ?>> stateDescriptorMap = new HashMap<>();
	private final Repeated isRepeated;

	DslTriggerInvokable(int id, DslTriggerInvokable<T, W> parent, DslTrigger<T, W> trigger) {
		Preconditions.checkNotNull(trigger);

		this.triggerId = id;
		this.parent = parent;
		this.trigger = trigger;
		this.context = new DslTriggerContext();
		this.isRepeated = this.trigger.isRepeated();
	}

	void setContext(Trigger.TriggerContext context) {
		this.context.setTtriggerContext(context, this);
	}

	/**
	 * This method traverses the tree of {@link DslTriggerInvokable invokables} towards the root
	 * until it finds the first that has a defined repeated flag. As soon as this is met, then
	 * the rest of the subtree tree has the same behavior.
	 */
	Repeated isRepeated() {
		return (parent == null || !isRepeated.isUndefined()) ? isRepeated : parent.isRepeated();
	}

	private DslTriggerInvokable<T, W> getChildInvokable(int index) {
		if (index >= children.size()) {
			throw new RuntimeException("Requested child at position " + index +
				" but trigger " + trigger + " has " + children.size() +" children.");
		}
		return this.children.isEmpty() ? null : this.children.get(index);
	}

	private <S extends State> StateDescriptor<S, ?> getStateDescriptor(StateDescriptor<S, ?> stateDescriptor) {
		StateDescriptor<S, ?> descriptor = (StateDescriptor<S, ?>) stateDescriptorMap.get(stateDescriptor.getName());
		if (descriptor == null) {
			throw new RuntimeException("Unknown state descriptor: " + stateDescriptor.getName());
		}
		return descriptor;
	}

	void addChild(DslTriggerInvokable<T, W> child) {
		this.children.add(child);
	}

	boolean invokeShouldFire(long time, boolean isEventTimeTimer, W window) throws Exception {
		return this.trigger.shouldFireOnTimer(time, isEventTimeTimer, window, context);
	}

	boolean invokeOnElement(T element, long timestamp, W window) throws Exception {
		return this.trigger.onElement(element, timestamp, window, context);
	}

	void invokeOnFire(W window) throws Exception {
		this.trigger.onFire(window, context);
	}

	boolean invokeOnMerge(W window) throws Exception {
		return this.trigger.onMerge(window, context);
	}

	void invokeClear(W window) throws Exception {
		this.trigger.clear(window, context);
	}

	<S extends State> StateDescriptor<S, ?> translateStateDescriptor(StateDescriptor<S, ?> stateDescriptor) {
		String descName = stateDescriptor.getName();
		StateDescriptor<S, ?> newStateDescriptor = (StateDescriptor<S, ?>) stateDescriptorMap.get(descName);
		if (newStateDescriptor == null) {
			newStateDescriptor = getStateDescriptorWithId(stateDescriptor);
			stateDescriptorMap.put(descName, newStateDescriptor);
		}
		return newStateDescriptor;
	}

	private <S extends State> StateDescriptor<S, ?> getStateDescriptorWithId(StateDescriptor<S, ?> stateDescriptor) {
		return StateDescriptor.translateStateDescriptorWithSuffix(stateDescriptor, "id=" + this.triggerId + ".trigger");
	}

	@Override
	public String toString() {
		return toStringWithPrefix("");
	}

	private String toStringWithPrefix(String prefix) {
		StringBuilder str = new StringBuilder();
		str.append(prefix).append(this.triggerId).append(" : ").append(this.trigger.toString());
		if (this.children != null) {
			for (DslTriggerInvokable invokable: this.children) {
				str.append("\n").append(invokable.toStringWithPrefix(prefix + "\t"));
			}
		}
		return str.toString();
	}

	private class DslTriggerContext implements DslTrigger.DslTriggerContext {

		private transient Trigger.TriggerContext ctx;

		@Override
		public void setTtriggerContext(Trigger.TriggerContext ctx, DslTriggerInvokable invokable) {
			if (ctx == null) {
				throw new RuntimeException("The TriggerContext has not been properly initialized.");
			}
			this.ctx = ctx;
		}

		@Override
		public DslTriggerInvokable getChildInvokable(int index) {
			DslTriggerInvokable child = DslTriggerInvokable.this.getChildInvokable(index);
			if (ctx == null) {
				throw new RuntimeException("The TriggerContext has not been properly initialized.");
			}
			child.setContext(ctx);
			return child;
		}

		@Override
		public long getCurrentProcessingTime() {
			if (ctx == null) {
				throw new RuntimeException("The TriggerContext has not been properly initialized.");
			}
			return ctx.getCurrentProcessingTime();
		}

		@Override
		public long getCurrentWatermark() {
			if (ctx == null) {
				throw new RuntimeException("The TriggerContext has not been properly initialized.");
			}
			return ctx.getCurrentWatermark();
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			if (ctx == null) {
				throw new RuntimeException("The TriggerContext has not been properly initialized.");
			}
			ctx.registerProcessingTimeTimer(time);
		}

		@Override
		public void registerEventTimeTimer(long time) {
			if (ctx == null) {
				throw new RuntimeException("The TriggerContext has not been properly initialized.");
			}
			ctx.registerEventTimeTimer(time);
		}

		@Override
		@SuppressWarnings("unchecked")
		public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			if (ctx == null) {
				throw new RuntimeException("The TriggerContext has not been properly initialized.");
			}
			try {
				StateDescriptor<S, ?> actualStateDescriptor = getStateDescriptor(stateDescriptor);
				return ctx.getPartitionedState(actualStateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve state.", e);
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		public <S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			if (ctx == null) {
				throw new RuntimeException("The TriggerContext has not been properly initialized.");
			} else if (!(ctx instanceof Trigger.OnMergeContext)) {
				throw new RuntimeException("The TriggerContext does not support merging.");
			}

			StateDescriptor<S, ?> actualStateDescriptor = getStateDescriptor(stateDescriptor);
			((Trigger.OnMergeContext) ctx).mergePartitionedState(actualStateDescriptor);
		}
	}
}
