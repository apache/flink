/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.window;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.window.assigners.CountSlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.CountTumblingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.InternalTimeWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.SessionWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.TumblingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.triggers.ElementTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.ProcessingTimeTriggers;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;
import org.apache.flink.table.types.logical.LogicalType;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base builder class for {@link AggregateWindowOperatorBuilder} or
 * {@link TableAggregateWindowOperator} which is used to build {@link WindowOperator} fluently.
 */
public abstract class WindowOperatorBuilder {
	protected LogicalType[] inputFieldTypes;
	protected WindowAssigner<?> windowAssigner;
	protected Trigger<?> trigger;
	protected LogicalType[] accumulatorTypes;
	protected LogicalType[] aggResultTypes;
	protected LogicalType[] windowPropertyTypes;
	protected long allowedLateness = 0L;
	protected boolean sendRetraction = false;
	protected int rowtimeIndex = -1;

	public WindowOperatorBuilder withInputFields(LogicalType[] inputFieldTypes) {
		this.inputFieldTypes = inputFieldTypes;
		return this;
	}

	public WindowOperatorBuilder tumble(Duration size) {
		checkArgument(windowAssigner == null);
		this.windowAssigner = TumblingWindowAssigner.of(size);
		return this;
	}

	public WindowOperatorBuilder sliding(Duration size, Duration slide) {
		checkArgument(windowAssigner == null);
		this.windowAssigner = SlidingWindowAssigner.of(size, slide);
		return this;
	}

	public WindowOperatorBuilder session(Duration sessionGap) {
		checkArgument(windowAssigner == null);
		this.windowAssigner = SessionWindowAssigner.withGap(sessionGap);
		return this;
	}

	public WindowOperatorBuilder countWindow(long size) {
		checkArgument(windowAssigner == null);
		checkArgument(trigger == null);
		this.windowAssigner = CountTumblingWindowAssigner.of(size);
		this.trigger = ElementTriggers.count(size);
		return this;
	}

	public WindowOperatorBuilder countWindow(long size, long slide) {
		checkArgument(windowAssigner == null);
		checkArgument(trigger == null);
		this.windowAssigner = CountSlidingWindowAssigner.of(size, slide);
		this.trigger = ElementTriggers.count(size);
		return this;
	}

	public WindowOperatorBuilder assigner(WindowAssigner<?> windowAssigner) {
		checkArgument(this.windowAssigner == null);
		checkNotNull(windowAssigner);
		this.windowAssigner = windowAssigner;
		return this;
	}

	public WindowOperatorBuilder triggering(Trigger<?> trigger) {
		checkNotNull(trigger);
		this.trigger = trigger;
		return this;
	}

	public WindowOperatorBuilder withEventTime(int rowtimeIndex) {
		checkNotNull(windowAssigner);
		checkArgument(windowAssigner instanceof InternalTimeWindowAssigner);
		InternalTimeWindowAssigner timeWindowAssigner = (InternalTimeWindowAssigner) windowAssigner;
		this.windowAssigner = (WindowAssigner<?>) timeWindowAssigner.withEventTime();
		this.rowtimeIndex = rowtimeIndex;
		if (trigger == null) {
			this.trigger = EventTimeTriggers.afterEndOfWindow();
		}
		return this;
	}

	public WindowOperatorBuilder withProcessingTime() {
		checkNotNull(windowAssigner);
		checkArgument(windowAssigner instanceof InternalTimeWindowAssigner);
		InternalTimeWindowAssigner timeWindowAssigner = (InternalTimeWindowAssigner) windowAssigner;
		this.windowAssigner = (WindowAssigner<?>) timeWindowAssigner.withProcessingTime();
		if (trigger == null) {
			this.trigger = ProcessingTimeTriggers.afterEndOfWindow();
		}
		return this;
	}

	public WindowOperatorBuilder withAllowedLateness(Duration allowedLateness) {
		checkArgument(!allowedLateness.isNegative());
		if (allowedLateness.toMillis() > 0) {
			this.allowedLateness = allowedLateness.toMillis();
			// allow late element, which means this window will send retractions
			this.sendRetraction = true;
		}
		return this;
	}

	public WindowOperatorBuilder withSendRetraction() {
		this.sendRetraction = true;
		return this;
	}

	protected void aggregate(
		LogicalType[] accumulatorTypes,
		LogicalType[] aggResultTypes,
		LogicalType[] windowPropertyTypes) {
		this.accumulatorTypes = accumulatorTypes;
		this.aggResultTypes = aggResultTypes;
		this.windowPropertyTypes = windowPropertyTypes;
	}

	@VisibleForTesting
	public abstract WindowOperatorBuilder aggregate(
			NamespaceAggsHandleFunctionBase<?> aggregateFunction,
			RecordEqualiser equaliser,
			LogicalType[] accumulatorTypes,
			LogicalType[] aggResultTypes,
			LogicalType[] windowPropertyTypes);

	public abstract WindowOperator build();
}
