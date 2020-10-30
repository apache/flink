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

package org.apache.flink.streaming.api.utils;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.typeutils.DataViewUtils;

import com.google.protobuf.ByteString;

import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.typeutils.PythonTypeUtils.toProtoType;

/**
 * The collectors used to collect Row values.
 */
public enum PythonOperatorUtils {
	;

	public static FlinkFnApi.UserDefinedFunction getUserDefinedFunctionProto(PythonFunctionInfo pythonFunctionInfo) {
		FlinkFnApi.UserDefinedFunction.Builder builder = FlinkFnApi.UserDefinedFunction.newBuilder();
		builder.setPayload(ByteString.copyFrom(pythonFunctionInfo.getPythonFunction().getSerializedPythonFunction()));
		for (Object input : pythonFunctionInfo.getInputs()) {
			FlinkFnApi.Input.Builder inputProto =
				FlinkFnApi.Input.newBuilder();
			if (input instanceof PythonFunctionInfo) {
				inputProto.setUdf(getUserDefinedFunctionProto((PythonFunctionInfo) input));
			} else if (input instanceof Integer) {
				inputProto.setInputOffset((Integer) input);
			} else {
				inputProto.setInputConstant(ByteString.copyFrom((byte[]) input));
			}
			builder.addInputs(inputProto);
		}
		return builder.build();
	}

	public static FlinkFnApi.UserDefinedAggregateFunction getUserDefinedAggregateFunctionProto(
		PythonAggregateFunctionInfo pythonFunctionInfo,
		DataViewUtils.DataViewSpec[] dataViewSpecs) {
		FlinkFnApi.UserDefinedAggregateFunction.Builder builder = FlinkFnApi.UserDefinedAggregateFunction.newBuilder();
		builder.setPayload(ByteString.copyFrom(pythonFunctionInfo.getPythonFunction().getSerializedPythonFunction()));
		builder.setDistinct(pythonFunctionInfo.isDistinct());
		builder.setFilterArg(pythonFunctionInfo.getFilterArg());
		for (Object input : pythonFunctionInfo.getInputs()) {
			FlinkFnApi.Input.Builder inputProto =
				FlinkFnApi.Input.newBuilder();
			if (input instanceof Integer) {
				inputProto.setInputOffset((Integer) input);
			} else {
				inputProto.setInputConstant(ByteString.copyFrom((byte[]) input));
			}
			builder.addInputs(inputProto);
		}
		if (dataViewSpecs != null) {
			for (DataViewUtils.DataViewSpec spec : dataViewSpecs) {
				FlinkFnApi.UserDefinedAggregateFunction.DataViewSpec.Builder specBuilder =
					FlinkFnApi.UserDefinedAggregateFunction.DataViewSpec.newBuilder();
				specBuilder.setName(spec.getStateId());
				if (spec instanceof DataViewUtils.ListViewSpec) {
					DataViewUtils.ListViewSpec listViewSpec = (DataViewUtils.ListViewSpec) spec;
					specBuilder.setListView(
						FlinkFnApi.UserDefinedAggregateFunction.DataViewSpec.ListView.newBuilder().setElementType(
							toProtoType(listViewSpec.getElementDataType().getLogicalType())));
				} else {
					DataViewUtils.MapViewSpec mapViewSpec = (DataViewUtils.MapViewSpec) spec;
					FlinkFnApi.UserDefinedAggregateFunction.DataViewSpec.MapView.Builder mapViewBuilder =
						FlinkFnApi.UserDefinedAggregateFunction.DataViewSpec.MapView.newBuilder();
					mapViewBuilder.setKeyType(toProtoType(mapViewSpec.getKeyDataType().getLogicalType()));
					mapViewBuilder.setValueType(toProtoType(mapViewSpec.getValueDataType().getLogicalType()));
					specBuilder.setMapView(mapViewBuilder.build());
				}
				specBuilder.setFieldIndex(spec.getFieldIndex());
				builder.addSpecs(specBuilder.build());
			}
		}
		return builder.build();
	}

	public static FlinkFnApi.UserDefinedDataStreamFunction getUserDefinedDataStreamFunctionProto(
		DataStreamPythonFunctionInfo dataStreamPythonFunctionInfo,
		RuntimeContext runtimeContext,
		Map<String, String> internalParameters) {
		FlinkFnApi.UserDefinedDataStreamFunction.Builder builder =
			FlinkFnApi.UserDefinedDataStreamFunction.newBuilder();
		builder.setFunctionType(FlinkFnApi.UserDefinedDataStreamFunction.FunctionType.forNumber(
			dataStreamPythonFunctionInfo.getFunctionType()));
		builder.setRuntimeContext(
			FlinkFnApi.UserDefinedDataStreamFunction.RuntimeContext.newBuilder()
				.setTaskName(runtimeContext.getTaskName())
				.setTaskNameWithSubtasks(runtimeContext.getTaskNameWithSubtasks())
				.setNumberOfParallelSubtasks(runtimeContext.getNumberOfParallelSubtasks())
				.setMaxNumberOfParallelSubtasks(runtimeContext.getMaxNumberOfParallelSubtasks())
				.setIndexOfThisSubtask(runtimeContext.getIndexOfThisSubtask())
				.setAttemptNumber(runtimeContext.getAttemptNumber())
				.addAllJobParameters(runtimeContext.getExecutionConfig().getGlobalJobParameters()
					.toMap().entrySet().stream().map(
						entry -> FlinkFnApi.UserDefinedDataStreamFunction.JobParameter.newBuilder()
							.setKey(entry.getKey())
							.setValue(entry.getValue())
							.build())
					.collect(Collectors.toList()))
				.addAllJobParameters(internalParameters.entrySet().stream().map(
					entry -> FlinkFnApi.UserDefinedDataStreamFunction.JobParameter.newBuilder()
						.setKey(entry.getKey())
						.setValue(entry.getValue())
						.build())
					.collect(Collectors.toList()))
				.build());
		builder.setPayload(ByteString.copyFrom(
			dataStreamPythonFunctionInfo.getPythonFunction().getSerializedPythonFunction()));
		return builder.build();
	}
}
