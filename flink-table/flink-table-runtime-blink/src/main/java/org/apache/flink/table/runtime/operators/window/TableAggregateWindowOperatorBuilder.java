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
import org.apache.flink.table.runtime.generated.GeneratedNamespaceTableAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunctionBase;
import org.apache.flink.table.runtime.generated.NamespaceTableAggsHandleFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link TableAggregateWindowOperatorBuilder} is used to build {@link TableAggregateWindowOperator} fluently.
 *
 * <pre>
 * TableAggregateWindowOperatorBuilder
 *   .builder(KeyedStream)
 *   .tumble(Duration.ofMinutes(1))	// sliding(...), session(...)
 *   .withEventTime()	// withProcessingTime()
 *   .aggregate(TableAggregationsFunction, accTypes, windowTypes)
 *   .build();
 * </pre>
 */
public final class TableAggregateWindowOperatorBuilder extends WindowOperatorBuilder {
	private NamespaceTableAggsHandleFunction<?> tableAggregateFunction;
	private GeneratedNamespaceTableAggsHandleFunction<?> generatedTableAggregateFunction;

	public static TableAggregateWindowOperatorBuilder builder() {
		return new TableAggregateWindowOperatorBuilder();
	}

	public TableAggregateWindowOperatorBuilder aggregate(
			NamespaceTableAggsHandleFunction<?> tableAggregateFunction,
			LogicalType[] accumulatorTypes,
			LogicalType[] aggResultTypes,
			LogicalType[] windowPropertyTypes) {
		ClosureCleaner.clean(tableAggregateFunction, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
		aggregate(accumulatorTypes, aggResultTypes, windowPropertyTypes);
		this.tableAggregateFunction = checkNotNull(tableAggregateFunction);
		return this;
	}

	public TableAggregateWindowOperatorBuilder aggregate(
			GeneratedNamespaceTableAggsHandleFunction<?> generatedTableAggregateFunction,
			LogicalType[] accumulatorTypes,
			LogicalType[] aggResultTypes,
			LogicalType[] windowPropertyTypes) {
		aggregate(accumulatorTypes, aggResultTypes, windowPropertyTypes);
		this.generatedTableAggregateFunction = checkNotNull(generatedTableAggregateFunction);
		return this;
	}

	@Override
	public WindowOperatorBuilder aggregate(
			NamespaceAggsHandleFunctionBase<?> aggregateFunction,
			RecordEqualiser equaliser,
			LogicalType[] accumulatorTypes,
			LogicalType[] aggResultTypes,
			LogicalType[] windowPropertyTypes) {
		return aggregate((NamespaceTableAggsHandleFunction) aggregateFunction,
			accumulatorTypes,
			aggResultTypes,
			windowPropertyTypes);
	}

	public WindowOperator build() {
		checkNotNull(trigger, "trigger is not set");
		if (generatedTableAggregateFunction != null) {
			//noinspection unchecked
			return new TableAggregateWindowOperator(
					generatedTableAggregateFunction,
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
			return new TableAggregateWindowOperator(
					tableAggregateFunction,
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
