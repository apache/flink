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

package org.apache.flink.table.runtime.window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.table.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.generated.RecordEqualiser;
import org.apache.flink.table.runtime.window.assigners.CountSlidingWindowAssigner;
import org.apache.flink.table.runtime.window.assigners.CountTumblingWindowAssigner;
import org.apache.flink.table.runtime.window.assigners.InternalTimeWindowAssigner;
import org.apache.flink.table.runtime.window.assigners.SessionWindowAssigner;
import org.apache.flink.table.runtime.window.assigners.SlidingWindowAssigner;
import org.apache.flink.table.runtime.window.assigners.TumblingWindowAssigner;
import org.apache.flink.table.runtime.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.window.triggers.ElementTriggers;
import org.apache.flink.table.runtime.window.triggers.EventTimeTriggers;
import org.apache.flink.table.runtime.window.triggers.ProcessingTimeTriggers;
import org.apache.flink.table.runtime.window.triggers.Trigger;
import org.apache.flink.table.types.logical.LogicalType;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link WindowOperatorBuilder} is used to build {@link WindowOperator} fluently.
 *
 * <pre>
 * WindowOperatorBuilder
 *   .builder(KeyedStream)
 *   .tumble(Duration.ofMinutes(1))	// sliding(...), session(...)
 *   .withEventTime()	// withProcessingTime()
 *   .aggregate(AggregationsFunction, accTypes, windowTypes)
 *   .withAllowedLateness(Duration.ZERO)
 *   .withSendRetraction()
 *   .build();
 * </pre>
 */
public class WindowOperatorBuilder {
	private LogicalType[] inputFieldTypes;
	private WindowAssigner<?> windowAssigner;
	private Trigger<?> trigger;
	private NamespaceAggsHandleFunction<?> aggregateFunction;
	private GeneratedNamespaceAggsHandleFunction<?> generatedAggregateFunction;
	private RecordEqualiser equaliser;
	private GeneratedRecordEqualiser generatedEqualiser;
	private LogicalType[] accumulatorTypes;
	private LogicalType[] aggResultTypes;
	private LogicalType[] windowPropertyTypes;
	private long allowedLateness = 0L;
	private boolean sendRetraction = false;
	private int rowtimeIndex = -1;

	public static WindowOperatorBuilder builder() {
		return new WindowOperatorBuilder();
	}

	public WindowOperatorBuilder withInputFields(LogicalType[] inputFieldTypes) {
		this.inputFieldTypes = inputFieldTypes;
		return this;
	}

	public WindowOperatorBuilder tumble(Duration size, long offset) {
		checkArgument(windowAssigner == null);
		this.windowAssigner = TumblingWindowAssigner.of(size)
				.withOffset(Duration.ofMillis(-offset));
		return this;
	}

	public WindowOperatorBuilder sliding(Duration size, Duration slide, long offset) {
		checkArgument(windowAssigner == null);
		this.windowAssigner = SlidingWindowAssigner.of(size, slide)
				.withOffset(Duration.ofMillis(-offset));
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

	public WindowOperatorBuilder aggregate(
			NamespaceAggsHandleFunction<?> aggregateFunction,
			RecordEqualiser equaliser,
			LogicalType[] accumulatorTypes,
			LogicalType[] aggResultTypes,
			LogicalType[] windowPropertyTypes) {
		ClosureCleaner.clean(aggregateFunction, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
		this.accumulatorTypes = accumulatorTypes;
		this.aggResultTypes = aggResultTypes;
		this.windowPropertyTypes = windowPropertyTypes;
		this.aggregateFunction = checkNotNull(aggregateFunction);
		this.equaliser = checkNotNull(equaliser);
		return this;
	}

	public WindowOperatorBuilder aggregate(
			GeneratedNamespaceAggsHandleFunction<?> generatedAggregateFunction,
			GeneratedRecordEqualiser generatedEqualiser,
			LogicalType[] accumulatorTypes,
			LogicalType[] aggResultTypes,
			LogicalType[] windowPropertyTypes) {
		this.accumulatorTypes = accumulatorTypes;
		this.aggResultTypes = aggResultTypes;
		this.windowPropertyTypes = windowPropertyTypes;
		this.generatedAggregateFunction = checkNotNull(generatedAggregateFunction);
		this.generatedEqualiser = checkNotNull(generatedEqualiser);
		return this;
	}

	public WindowOperatorBuilder withSendRetraction() {
		this.sendRetraction = true;
		return this;
	}

	public WindowOperator build() {
		checkNotNull(trigger, "trigger is not set");
		if (generatedAggregateFunction != null && generatedEqualiser != null) {
			//noinspection unchecked
			return new WindowOperator(
					generatedAggregateFunction,
					generatedEqualiser,
					windowAssigner,
					trigger,
					windowAssigner.getWindowSerializer(new ExecutionConfig()),
					inputFieldTypes,
					accumulatorTypes,
					aggResultTypes,
					windowPropertyTypes,
					rowtimeIndex,
					sendRetraction,
					allowedLateness);
		} else {
			//noinspection unchecked
			return new WindowOperator(
					aggregateFunction,
					equaliser,
					windowAssigner,
					trigger,
					windowAssigner.getWindowSerializer(new ExecutionConfig()),
					inputFieldTypes,
					accumulatorTypes,
					aggResultTypes,
					windowPropertyTypes,
					rowtimeIndex,
					sendRetraction,
					allowedLateness);
		}
	}
}
