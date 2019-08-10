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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.types.logical.LogicalType;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link AggregateWindowOperatorBuilder} is used to build {@link AggregateWindowOperator} fluently.
 *
 * <pre>
 * AggregateWindowOperatorBuilder
 *   .builder(KeyedStream)
 *   .tumble(Duration.ofMinutes(1))	// sliding(...), session(...)
 *   .withEventTime()	// withProcessingTime()
 *   .aggregate(AggregationsFunction, accTypes, windowTypes)
 *   .withAllowedLateness(Duration.ZERO)
 *   .withSendRetraction()
 *   .build();
 * </pre>
 */
public final class AggregateWindowOperatorBuilder extends WindowOperatorBuilder {
	private NamespaceAggsHandleFunction<?> aggregateFunction;
	private GeneratedNamespaceAggsHandleFunction<?> generatedAggregateFunction;
	private RecordEqualiser equaliser;
	private GeneratedRecordEqualiser generatedEqualiser;

	public static AggregateWindowOperatorBuilder builder() {
		return new AggregateWindowOperatorBuilder();
	}

	public AggregateWindowOperatorBuilder withAllowedLateness(Duration allowedLateness) {
		checkArgument(!allowedLateness.isNegative());
		if (allowedLateness.toMillis() > 0) {
			this.allowedLateness = allowedLateness.toMillis();
			// allow late element, which means this window will send retractions
			this.sendRetraction = true;
		}
		return this;
	}

	public AggregateWindowOperatorBuilder aggregate(
			NamespaceAggsHandleFunction<?> aggregateFunction,
			RecordEqualiser equaliser,
			LogicalType[] accumulatorTypes,
			LogicalType[] aggResultTypes,
			LogicalType[] windowPropertyTypes) {
		ClosureCleaner.clean(aggregateFunction, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
		aggregate(accumulatorTypes, aggResultTypes, windowPropertyTypes);
		this.aggregateFunction = checkNotNull(aggregateFunction);
		this.equaliser = checkNotNull(equaliser);
		return this;
	}

	public AggregateWindowOperatorBuilder aggregate(
			GeneratedNamespaceAggsHandleFunction<?> generatedAggregateFunction,
			GeneratedRecordEqualiser generatedEqualiser,
			LogicalType[] accumulatorTypes,
			LogicalType[] aggResultTypes,
			LogicalType[] windowPropertyTypes) {
		aggregate(accumulatorTypes, aggResultTypes, windowPropertyTypes);
		this.generatedAggregateFunction = checkNotNull(generatedAggregateFunction);
		this.generatedEqualiser = checkNotNull(generatedEqualiser);
		return this;
	}

	@Override
	public WindowOperatorBuilder aggregate(
			NamespaceAggsHandleFunctionBase<?> aggregateFunction,
			RecordEqualiser equaliser,
			LogicalType[] accumulatorTypes,
			LogicalType[] aggResultTypes,
			LogicalType[] windowPropertyTypes) {
		return aggregate((NamespaceAggsHandleFunction) aggregateFunction,
			equaliser,
			accumulatorTypes,
			aggResultTypes,
			windowPropertyTypes);
	}

	public AggregateWindowOperator build() {
		checkNotNull(trigger, "trigger is not set");
		if (generatedAggregateFunction != null && generatedEqualiser != null) {
			//noinspection unchecked
			return new AggregateWindowOperator(
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
			return new AggregateWindowOperator(
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
