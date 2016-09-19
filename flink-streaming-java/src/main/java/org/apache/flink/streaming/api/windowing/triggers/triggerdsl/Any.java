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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link DslTrigger} that fires if at least one of its children propose to fire.
 * @param <W> The type of {@link Window Windows} on which this {@code DslTrigger} can operate.
 */
public class Any<W extends Window> extends DslTrigger<Object, W> {
	private static final long serialVersionUID = 1L;

	private final List<DslTrigger<Object, W>> children;

	private Any(DslTrigger<Object, W>... triggers) {
		Preconditions.checkNotNull(triggers, "The list of triggers must not be null");
		Preconditions.checkNotNull(triggers.length == 0, "The list of triggers must not be empty");

		this.children = Arrays.asList(triggers);
		this.setIsRepeated(Repeated.ONCE);
	}

	@Override
	List<DslTrigger<Object, W>> getChildTriggers() {
		return children;
	}

	@Override
	DslTrigger<Object, W> translate(TypeSerializer<W> windowSerializer, long allowedLateness) {
		return this;
	}

	@Override
	<S extends State> List<StateDescriptor<S, ?>> getStateDescriptors() {
		return Collections.emptyList();
	}

	@Override
	boolean onElement(Object element, long timestamp, W window, DslTriggerContext context) throws Exception {
		boolean triggerResult = false;
		for (int childIdx = 0; childIdx < children.size(); childIdx++) {
			DslTriggerInvokable<Object, W> triggerInvokable = context.getChildInvokable(childIdx);
			boolean part = triggerInvokable.invokeOnElement(element, timestamp, window);
			triggerResult = triggerResult || part;
		}
		return triggerResult;
	}

	@Override
	boolean shouldFireOnTimer(long time, boolean isEventTimeTimer, W window, DslTriggerContext context) throws Exception {
		boolean shouldFire = false;
		for (int childIdx = 0; childIdx < children.size(); childIdx++) {
			DslTriggerInvokable<Object, W> triggerInvokable = context.getChildInvokable(childIdx);
			boolean part = triggerInvokable.invokeShouldFire(time, isEventTimeTimer, window);
			shouldFire = shouldFire || part;
		}
		return shouldFire;
	}

	@Override
	void onFire(W window, DslTriggerContext context) throws Exception {
		for (int childIdx = 0; childIdx < children.size(); childIdx++) {
			DslTriggerInvokable<Object, W> triggerInvokable = context.getChildInvokable(childIdx);
			triggerInvokable.invokeOnFire(window);
		}
	}

	@Override
	public boolean canMerge() {
		boolean canMerge = true;
		for (DslTrigger trigger: this.children) {
			canMerge = canMerge && trigger.canMerge();
		}
		return canMerge;
	}

	@Override
	boolean onMerge(W window, DslTriggerContext context) throws Exception {
		boolean triggerResult = false;
		for (int childIdx = 0; childIdx < children.size(); childIdx++) {
			DslTriggerInvokable<Object, W> triggerInvokable = context.getChildInvokable(childIdx);
			triggerResult = triggerResult || triggerInvokable.invokeOnMerge(window);
		}
		return triggerResult;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append("Any.of(");
		Iterator<DslTrigger<Object, W>> it = children.iterator();
		for (;;) {
			DslTrigger<Object, W> trigger = it.next();
			str.append(trigger);
			if (!it.hasNext()) {
				return str.append(")").toString();
			}
			str.append(", ");
		}
	}

	@Override
	void clear(W window, DslTriggerContext context) throws Exception {
		for (int childIdx = 0; childIdx < children.size(); childIdx++) {
			DslTriggerInvokable<Object, W> triggerInvokable = context.getChildInvokable(childIdx);
			triggerInvokable.invokeClear(window);
		}
	}

	public static <W extends Window> Any<W> of(DslTrigger<Object, W>... triggers) {
		return new Any<>(triggers);
	}
}
