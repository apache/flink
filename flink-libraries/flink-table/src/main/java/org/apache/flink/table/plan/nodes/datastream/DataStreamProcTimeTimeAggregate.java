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

package org.apache.flink.table.plan.nodes.datastream;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.plan.logical.rel.util.WindowAggregateUtil;
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel;
import org.apache.flink.table.plan.nodes.datastream.function.DataStreamProcTimeAggregateGlobalWindowFunction;
import org.apache.flink.table.plan.nodes.datastream.function.DataStreamProcTimeAggregateWindowFunction;
import org.apache.flink.types.Row;

/**
 * Flink RelNode which matches along with Aggregates over Sliding Window with
 * time bounds.
 *
 */

public class DataStreamProcTimeTimeAggregate extends DataStreamRelJava {

	private LogicalWindow windowReference;
	private String description;

	public DataStreamProcTimeTimeAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
			RelDataType rowType, String description, LogicalWindow windowReference) {
		super(cluster, traitSet, input);

		this.rowType = rowType;
		this.description = description;
		this.windowReference = windowReference;

	}

	@Override
	protected RelDataType deriveRowType() {
		// TODO Auto-generated method stub
		return super.deriveRowType();
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, java.util.List<RelNode> inputs) {

		if (inputs.size() != 1) {
			System.err.println(this.getClass().getName() + " : Input size must be one!");
		}

		return new DataStreamProcTimeTimeAggregate(getCluster(), traitSet, inputs.get(0), getRowType(),
				getDescription(), windowReference);

	}

	@Override
	public DataStream<Row> translateToPlan(StreamTableEnvironment tableEnv) {

		// Get the general parameters related to the datastream, inputs, result
		DataStream<Row> inputDataStream = ((DataStreamRel) getInput()).translateToPlan(tableEnv);

		if (getRowType() == null) {
			throw new IllegalArgumentException("Type must be defined");
		}

		List<RelDataTypeField> fieldList = getRowType().getFieldList();

		if (fieldList.size() < 1) {
			throw new IllegalArgumentException("Fields must be defined");
		}

		TypeInformation<?>[] rowType = new TypeInformation<?>[fieldList.size()];
		int i = 0;
		for (RelDataTypeField field : fieldList) {
			rowType[i] = FlinkTypeFactory.toTypeInfo(field.getType());
			i++;
		}
		TypeInformation<Row> returnType = new RowTypeInfo(rowType);

		// WindowUtil object to help with operations on the LogicalWindow object
		WindowAggregateUtil windowUtil = new WindowAggregateUtil(windowReference);
		int[] partitionKeys = windowUtil.getPartitions();

		// get aggregations
		List<TypeInformation<?>> typeOutput = new ArrayList<>();
		List<TypeInformation<?>> typeInput = new ArrayList<>();
		List<String> aggregators = new ArrayList<>();
		List<Integer> indexes = new ArrayList<>();
		windowUtil.getAggregations(aggregators, typeOutput, indexes, typeInput);

		// final long time_boundary =
		// Long.parseLong(windowReference.getConstants().get(1).getValue().toString());
		final long time_boundary = windowUtil.getLowerBoundary(getInput());
		DataStream<Row> aggregateWindow = null;

		// As we it is not possible to operate neither on sliding count neither
		// on sliding time we need to manage the eviction of the events that
		// expire ourselves based on the proctime (system time). Therefore the
		// current system time is assign as the timestamp of the event to be
		// recognize by the evictor
		@SuppressWarnings({ "rawtypes", "unchecked" })
		DataStream<Row> inputDataStreamTimed = inputDataStream
				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks() {
					private static final long serialVersionUID = 1L;

					@Override
					public Watermark getCurrentWatermark() {
						return null;
					}

					@Override
					public long extractTimestamp(Object element, long previousElementTimestamp) {
						return System.currentTimeMillis();

					}
				});

		// null indicates non partitioned window
		if (partitionKeys == null) {

			aggregateWindow = inputDataStreamTimed.windowAll(GlobalWindows.create()).trigger(CountTrigger.of(1))
					.evictor(TimeEvictor.of(Time.milliseconds(time_boundary)))
					.apply(new DataStreamProcTimeAggregateGlobalWindowFunction(aggregators, indexes, typeOutput,
							typeInput))
					.returns((TypeInformation<Row>) returnType);

		} else {

			aggregateWindow = inputDataStreamTimed.keyBy(partitionKeys).window(GlobalWindows.create())
					.trigger(CountTrigger.of(1)).evictor(TimeEvictor.of(Time.milliseconds(time_boundary)))
					.apply(new DataStreamProcTimeAggregateWindowFunction(aggregators, indexes, typeOutput, typeInput))
					.returns((TypeInformation<Row>) returnType);
		}

		return aggregateWindow;

	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString() + "(" + "window=[" + windowReference + "]" + ")";
	}

}
