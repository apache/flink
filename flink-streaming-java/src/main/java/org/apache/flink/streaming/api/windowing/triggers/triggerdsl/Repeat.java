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

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collections;
import java.util.List;

public class Repeat<T, W extends Window> extends DslTrigger<T, W> {
	private static final long serialVersionUID = 1L;

	private final DslTrigger<T, W> trigger;

	private ValueStateDescriptor<Byte> hasFiredStateDesc;

	private Repeat(DslTrigger<T, W> trigger, boolean fireForever) {
		this.trigger = trigger;
		this.setIsRepeated(fireForever ? Repeated.FOREVER : Repeated.ONCE);
	}

	@Override
	List<DslTrigger<T, W>> getChildTriggers() {
		return Collections.singletonList(trigger);
	}

	@Override
	DslTrigger<T, W> translate(TypeSerializer<W> windowSerializer, long allowedLateness) {
		this.hasFiredStateDesc = this.isRepeated().once() ?
			new ValueStateDescriptor<>("repeat-once", ByteSerializer.INSTANCE, (byte) 0) : null;
		return this;
	}

	@Override
	List<ValueStateDescriptor<Byte>> getStateDescriptors() {
		return hasFiredStateDesc != null ?
			Collections.singletonList(hasFiredStateDesc) :
			Collections.<ValueStateDescriptor<Byte>>emptyList();
	}

	@Override
	boolean shouldFireOnTimer(long time, boolean isEventTimeTimer, W window, DslTriggerContext context) throws Exception {
		boolean shouldChildFire = context.getChildInvokable(0).invokeShouldFire(time, isEventTimeTimer, window);
		if (hasFiredStateDesc == null) {
			return shouldChildFire;
		}

		boolean hasFiredAlready = context.getPartitionedState(hasFiredStateDesc).value() == (byte) 1;
		return !hasFiredAlready && shouldChildFire;
	}

	@Override
	boolean onElement(T element, long timestamp, W window, DslTriggerContext context) throws Exception {
		boolean childTriggerResult = context.getChildInvokable(0).invokeOnElement(element, timestamp, window);
		if (hasFiredStateDesc == null) {
			return childTriggerResult;
		}

		boolean hasFiredAlready = context.getPartitionedState(hasFiredStateDesc).value() == (byte) 1;
		return !hasFiredAlready && childTriggerResult;
	}

	@Override
	void onFire(W window, DslTriggerContext context) throws Exception {
		context.getChildInvokable(0).invokeOnFire(window);
		if (hasFiredStateDesc != null) {
			context.getPartitionedState(hasFiredStateDesc).update((byte) 1);
		}
	}

	@Override
	void clear(W window, DslTriggerContext context) throws Exception {
		context.getChildInvokable(0).invokeClear(window);
		if (hasFiredStateDesc != null) {
			context.getPartitionedState(hasFiredStateDesc).clear();
		}
	}

	@Override
	public boolean canMerge() {
		return trigger.canMerge();
	}

	@Override
	boolean onMerge(W window, DslTriggerContext context) throws Exception {
		boolean childTriggerResult = context.getChildInvokable(0).invokeOnMerge(window);
		if (hasFiredStateDesc != null) {
			// even if we have fired for the old windows, we have not for the new
			context.getPartitionedState(hasFiredStateDesc).clear();
		}
		return childTriggerResult;
	}

	@Override
	public String toString() {
		return hasFiredStateDesc != null ? trigger.toString() : "Repeat.Forever(" + trigger.toString() + ")";
	}

	public static <T, W extends Window> DslTrigger<T, W> Forever(DslTrigger<T, W> trigger) {
		return new Repeat<>(trigger, true);
	}

	static <T, W extends Window> DslTrigger<T, W> Once(DslTrigger<T, W> trigger) {
		return new Repeat<>(trigger, false);
	}
}
