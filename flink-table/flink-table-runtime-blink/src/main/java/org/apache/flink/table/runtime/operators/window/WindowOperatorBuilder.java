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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceTableAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.generated.NamespaceTableAggsHandleFunction;
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
 * The {@link WindowOperatorBuilder} is used to build {@link WindowOperator} fluently.
 *
 * <p>Note: You have to call the aggregate method before the last build method.
 *
 * <pre>
 * WindowOperatorBuilder
 *   .builder(KeyedStream)
 *   .tumble(Duration.ofMinutes(1))	// sliding(...), session(...)
 *   .withEventTime()	// withProcessingTime()
 *   .withAllowedLateness(Duration.ZERO)
 *   .withSendRetraction()
 *   .aggregate(AggregationsFunction, accTypes, windowTypes)
 *   .build();
 * </pre>
 */
public class WindowOperatorBuilder {
	protected LogicalType[] inputFieldTypes;
	protected WindowAssigner<?> windowAssigner;
	protected Trigger<?> trigger;
	protected LogicalType[] accumulatorTypes;
	protected LogicalType[] aggResultTypes;
	protected LogicalType[] windowPropertyTypes;
	protected long allowedLateness = 0L;
	protected boolean sendRetraction = false;
	protected int rowtimeIndex = -1;

	public static WindowOperatorBuilder builder() {
		return new WindowOperatorBuilder();
	}

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

	public AggregateWindowOperatorBuilder aggregate(
		NamespaceAggsHandleFunction<?> aggregateFunction,
		RecordEqualiser equaliser,
		LogicalType[] accumulatorTypes,
		LogicalType[] aggResultTypes,
		LogicalType[] windowPropertyTypes) {

		aggregate(accumulatorTypes, aggResultTypes, windowPropertyTypes);
		return new AggregateWindowOperatorBuilder(aggregateFunction, equaliser, this);
	}

	public AggregateWindowOperatorBuilder aggregate(
		GeneratedNamespaceAggsHandleFunction<?> generatedAggregateFunction,
		GeneratedRecordEqualiser generatedEqualiser,
		LogicalType[] accumulatorTypes,
		LogicalType[] aggResultTypes,
		LogicalType[] windowPropertyTypes) {

		aggregate(accumulatorTypes, aggResultTypes, windowPropertyTypes);
		return new AggregateWindowOperatorBuilder(generatedAggregateFunction, generatedEqualiser, this);
	}

	public TableAggregateWindowOperatorBuilder aggregate(
		NamespaceTableAggsHandleFunction<?> tableAggregateFunction,
		LogicalType[] accumulatorTypes,
		LogicalType[] aggResultTypes,
		LogicalType[] windowPropertyTypes) {

		aggregate(accumulatorTypes, aggResultTypes, windowPropertyTypes);
		return new TableAggregateWindowOperatorBuilder(tableAggregateFunction, this);
	}

	public TableAggregateWindowOperatorBuilder aggregate(
		GeneratedNamespaceTableAggsHandleFunction<?> generatedTableAggregateFunction,
		LogicalType[] accumulatorTypes,
		LogicalType[] aggResultTypes,
		LogicalType[] windowPropertyTypes) {

		aggregate(accumulatorTypes, aggResultTypes, windowPropertyTypes);
		return new TableAggregateWindowOperatorBuilder(generatedTableAggregateFunction, this);
	}

	@VisibleForTesting
	WindowOperator aggregateAndBuild(
		NamespaceAggsHandleFunctionBase<?> aggregateFunction,
		RecordEqualiser equaliser,
		LogicalType[] accumulatorTypes,
		LogicalType[] aggResultTypes,
		LogicalType[] windowPropertyTypes) {
		aggregate(accumulatorTypes, aggResultTypes, windowPropertyTypes);
		if (aggregateFunction instanceof NamespaceAggsHandleFunction) {
			return new AggregateWindowOperatorBuilder(
				(NamespaceAggsHandleFunction) aggregateFunction, equaliser, this).build();
		} else {
			return new TableAggregateWindowOperatorBuilder(
				(NamespaceTableAggsHandleFunction) aggregateFunction, this).build();
		}
	}

	/**
	 * The builder which is used to build {@link TableAggregateWindowOperator} fluently.
	 */
	public static class TableAggregateWindowOperatorBuilder {
		private NamespaceTableAggsHandleFunction<?> tableAggregateFunction;
		private GeneratedNamespaceTableAggsHandleFunction<?> generatedTableAggregateFunction;
		private WindowOperatorBuilder windowOperatorBuilder;

		public TableAggregateWindowOperatorBuilder(
			NamespaceTableAggsHandleFunction<?> tableAggregateFunction,
			WindowOperatorBuilder windowOperatorBuilder) {
			this.tableAggregateFunction = tableAggregateFunction;
			this.windowOperatorBuilder = windowOperatorBuilder;
		}

		public TableAggregateWindowOperatorBuilder(
			GeneratedNamespaceTableAggsHandleFunction<?> generatedTableAggregateFunction,
			WindowOperatorBuilder windowOperatorBuilder) {
			this.generatedTableAggregateFunction = generatedTableAggregateFunction;
			this.windowOperatorBuilder = windowOperatorBuilder;
		}

		public WindowOperator build() {
			checkNotNull(windowOperatorBuilder.trigger, "trigger is not set");
			if (generatedTableAggregateFunction != null) {
				//noinspection unchecked
				return new TableAggregateWindowOperator(
					generatedTableAggregateFunction,
					windowOperatorBuilder.windowAssigner,
					windowOperatorBuilder.trigger,
					windowOperatorBuilder.windowAssigner.getWindowSerializer(new ExecutionConfig()),
					windowOperatorBuilder.inputFieldTypes,
					windowOperatorBuilder.accumulatorTypes,
					windowOperatorBuilder.aggResultTypes,
					windowOperatorBuilder.windowPropertyTypes,
					windowOperatorBuilder.rowtimeIndex,
					windowOperatorBuilder.sendRetraction,
					windowOperatorBuilder.allowedLateness);
			} else {
				//noinspection unchecked
				return new TableAggregateWindowOperator(
					tableAggregateFunction,
					windowOperatorBuilder.windowAssigner,
					windowOperatorBuilder.trigger,
					windowOperatorBuilder.windowAssigner.getWindowSerializer(new ExecutionConfig()),
					windowOperatorBuilder.inputFieldTypes,
					windowOperatorBuilder.accumulatorTypes,
					windowOperatorBuilder.aggResultTypes,
					windowOperatorBuilder.windowPropertyTypes,
					windowOperatorBuilder.rowtimeIndex,
					windowOperatorBuilder.sendRetraction,
					windowOperatorBuilder.allowedLateness);
			}
		}
	}

	/**
	 * The builder which is used to build {@link AggregateWindowOperator} fluently.
	 */
	public static class AggregateWindowOperatorBuilder {
		private NamespaceAggsHandleFunction<?> aggregateFunction;
		private GeneratedNamespaceAggsHandleFunction<?> generatedAggregateFunction;
		private RecordEqualiser equaliser;
		private GeneratedRecordEqualiser generatedEqualiser;
		private WindowOperatorBuilder windowOperatorBuilder;

		public AggregateWindowOperatorBuilder(
			GeneratedNamespaceAggsHandleFunction<?> generatedAggregateFunction,
			GeneratedRecordEqualiser generatedEqualiser,
			WindowOperatorBuilder windowOperatorBuilder) {
			this.generatedAggregateFunction = generatedAggregateFunction;
			this.generatedEqualiser = generatedEqualiser;
			this.windowOperatorBuilder = windowOperatorBuilder;
		}

		public AggregateWindowOperatorBuilder(
			NamespaceAggsHandleFunction<?> aggregateFunction,
			RecordEqualiser equaliser,
			WindowOperatorBuilder windowOperatorBuilder) {
			this.aggregateFunction = aggregateFunction;
			this.equaliser = equaliser;
			this.windowOperatorBuilder = windowOperatorBuilder;
		}

		public AggregateWindowOperator build() {
			checkNotNull(windowOperatorBuilder.trigger, "trigger is not set");
			if (generatedAggregateFunction != null && generatedEqualiser != null) {
				//noinspection unchecked
				return new AggregateWindowOperator(
					generatedAggregateFunction,
					generatedEqualiser,
					windowOperatorBuilder.windowAssigner,
					windowOperatorBuilder.trigger,
					windowOperatorBuilder.windowAssigner.getWindowSerializer(new ExecutionConfig()),
					windowOperatorBuilder.inputFieldTypes,
					windowOperatorBuilder.accumulatorTypes,
					windowOperatorBuilder.aggResultTypes,
					windowOperatorBuilder.windowPropertyTypes,
					windowOperatorBuilder.rowtimeIndex,
					windowOperatorBuilder.sendRetraction,
					windowOperatorBuilder.allowedLateness);
			} else {
				//noinspection unchecked
				return new AggregateWindowOperator(
					aggregateFunction,
					equaliser,
					windowOperatorBuilder.windowAssigner,
					windowOperatorBuilder.trigger,
					windowOperatorBuilder.windowAssigner.getWindowSerializer(new ExecutionConfig()),
					windowOperatorBuilder.inputFieldTypes,
					windowOperatorBuilder.accumulatorTypes,
					windowOperatorBuilder.aggResultTypes,
					windowOperatorBuilder.windowPropertyTypes,
					windowOperatorBuilder.rowtimeIndex,
					windowOperatorBuilder.sendRetraction,
					windowOperatorBuilder.allowedLateness);
			}
		}
	}
}
