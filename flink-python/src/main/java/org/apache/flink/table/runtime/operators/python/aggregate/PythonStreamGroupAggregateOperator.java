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

package org.apache.flink.table.runtime.operators.python.aggregate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.types.logical.RowType;

/**
 * The Python AggregateFunction operator for the blink planner.
 */
@Internal
public class PythonStreamGroupAggregateOperator extends AbstractPythonStreamAggregateOperator {

	private static final long serialVersionUID = 1L;

	@VisibleForTesting
	protected static final String STREAM_GROUP_AGGREGATE_URN = "flink:transform:stream_group_aggregate:v1";

	private final PythonAggregateFunctionInfo[] aggregateFunctions;

	private final DataViewUtils.DataViewSpec[][] dataViewSpecs;

	/**
	 * True if the count(*) agg is inserted by the planner.
	 */
	private final boolean countStarInserted;

	public PythonStreamGroupAggregateOperator(
		Configuration config,
		RowType inputType,
		RowType outputType,
		PythonAggregateFunctionInfo[] aggregateFunctions,
		DataViewUtils.DataViewSpec[][] dataViewSpecs,
		int[] grouping,
		int indexOfCountStar,
		boolean countStarInserted,
		boolean generateUpdateBefore,
		long minRetentionTime,
		long maxRetentionTime) {
		super(config, inputType, outputType, grouping, indexOfCountStar, generateUpdateBefore,
			minRetentionTime, maxRetentionTime);
		this.aggregateFunctions = aggregateFunctions;
		this.dataViewSpecs = dataViewSpecs;
		this.countStarInserted = countStarInserted;
	}

	@Override
	public PythonEnv getPythonEnv() {
		return aggregateFunctions[0].getPythonFunction().getPythonEnv();
	}

	/**
	 * Gets the proto representation of the Python user-defined aggregate functions to be executed.
	 */
	@Override
	public FlinkFnApi.UserDefinedAggregateFunctions getUserDefinedFunctionsProto() {
		FlinkFnApi.UserDefinedAggregateFunctions.Builder builder =
			super.getUserDefinedFunctionsProto().toBuilder();
		for (int i = 0; i < aggregateFunctions.length; i++) {
			DataViewUtils.DataViewSpec[] specs = null;
			if (i < dataViewSpecs.length) {
				specs = dataViewSpecs[i];
			}
			builder.addUdfs(
				PythonOperatorUtils.getUserDefinedAggregateFunctionProto(aggregateFunctions[i], specs));
		}
		builder.setCountStarInserted(countStarInserted);
		return builder.build();
	}

	@Override
	public String getFunctionUrn() {
		return STREAM_GROUP_AGGREGATE_URN;
	}
}
