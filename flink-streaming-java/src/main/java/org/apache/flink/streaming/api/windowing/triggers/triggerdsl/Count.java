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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link DslTrigger} that fires once the count of elements in a pane reaches or exceeds the given count.
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 */
@PublicEvolving
public class Count<W extends Window> extends DslTrigger<Object, W> {
	private static final long serialVersionUID = 1L;

	private final long maxCount;

	private ReducingStateDescriptor<Long> stateDesc;

	private Count(long maxCount) {
		this.maxCount = maxCount;
		this.setIsRepeated(Repeated.ONCE);
	}

	@Override
	List<DslTrigger<Object, W>> getChildTriggers() {
		return new ArrayList<>();
	}

	@Override
	DslTrigger<Object, W> translate(TypeSerializer<W> windowSerializer, long allowedLateness) {
		String descriptorName = "count_" + maxCount;
		this.stateDesc = new ReducingStateDescriptor<>(descriptorName, new Sum(), LongSerializer.INSTANCE);
		return this;
	}

	@Override
	List<ReducingStateDescriptor<Long>> getStateDescriptors() {
		if (stateDesc == null) {
			throw new IllegalStateException("The state descriptor has not been initialized. " +
				"Please call translate() before asking for state descriptors.");
		}
		List<ReducingStateDescriptor<Long>> descriptors = new ArrayList<>();
		descriptors.add(this.stateDesc);
		return descriptors;
	}

	@Override
	boolean shouldFireOnTimer(long time, boolean isEventTimeTimer, W window, DslTriggerContext context) throws Exception {

		// the count.get() != null is for cases where the state has not been initialized,
		// e.g. a count late trigger on a window with no late elements. In this case, the
		// cleanup timer will fire and we will have an NPE here if we do not check.
		ReducingState<Long> count = context.getPartitionedState(stateDesc);
		Long counter = count.get();
		return counter != null && counter >= maxCount;
	}

	@Override
	boolean onElement(Object element, long timestamp, W window, DslTriggerContext context) throws Exception {
		ReducingState<Long> count = context.getPartitionedState(stateDesc);
		count.add(1L);
		return count.get() >= maxCount;
	}

	@Override
	void clear(W window, DslTriggerContext context) throws Exception {
		context.getPartitionedState(stateDesc).clear();
	}

	@Override
	void onFire(Window window, DslTriggerContext context) throws Exception {
		context.getPartitionedState(stateDesc).clear();
	}

	@Override
	public boolean canMerge() {
		return true;
	}

	@Override
	boolean onMerge(W window, DslTriggerContext context) throws Exception {
		context.mergePartitionedState(stateDesc);
		ReducingState<Long> countState = context.getPartitionedState(stateDesc);
		Long count = countState.get();
		return (count != null && count >= maxCount);
	}

	@Override
	public String toString() {
		return "Count.atLeast(" +  maxCount + ")";
	}

	/**
	 * Creates a trigger that fires once the number of elements in a pane reaches the given count.
	 *
	 * @param maxCount The count of elements at which to fire.
	 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
	 */
	public static <W extends Window> Count<W> atLeast(long maxCount) {
		return new Count<>(maxCount);
	}

	private static class Sum implements ReduceFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return value1 + value2;
		}
	}
}
