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
import java.util.Optional;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlDialect;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.plan.logical.rel.util.WindowAggregateUtil;
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel;
import org.apache.flink.table.plan.nodes.datastream.DataStreamRelJava;
import org.apache.flink.table.plan.nodes.datastream.function.DataStreamProcTimeAggregateRowWindowFunction;
import org.apache.flink.table.typeutils.TypeConverter;

import scala.Option;

public class DataStreamProcTimeRowAggregate extends DataStreamRelJava {

	protected LogicalWindow windowRef;
	protected String description;
	protected WindowAggregateUtil winUtil;

	public DataStreamProcTimeRowAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
			RelDataType rowType, String description, LogicalWindow window) {
		super(cluster, traitSet, input);
		this.windowRef = window;
		this.rowType = rowType;
		this.description = description;
		this.winUtil = new WindowAggregateUtil();
	}

	@Override
	protected RelDataType deriveRowType() {
		return super.deriveRowType();
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, java.util.List<RelNode> inputs) {
		
		if(inputs.size()!=1){
			System.err.println(this.getClass().getName()+" : Input size must be one!");
		}
		
		return new DataStreamProcTimeRowAggregate(
				getCluster(), 
				traitSet, 
				inputs.get(0),
				getRowType(), getDescription(), windowRef);
	}

	@Override
	public DataStream<Object> translateToPlan(StreamTableEnvironment tableEnv,
			Option<TypeInformation<Object>> expectedType, Object ignore) {

		TableConfig config = tableEnv.getConfig();
		
		Option<TypeInformation<Object>> obj = Option.empty();
		DataStream<Object> inputDS = ((DataStreamRel) getInput()).translateToPlan(tableEnv, obj);

		System.out.println(inputDS);

		TypeInformation<?> returnType = TypeConverter.determineReturnType(getRowType(), expectedType,
				config.getNullCheck(), config.getEfficientTypeUsage());

		System.out.println(returnType);

		DataStream<Object> aggregateWindow = null;
		
		// TODO check type and return type consistency

		KeyedStream<Object, Tuple> keyedS = null;
		// apply partitions
		if (winUtil.isStreamPartitioned(windowRef)) {
			for (final Group group : windowRef.groups) {
				keyedS = inputDS.keyBy(winUtil.getKeysAsArray(group));
				
				int lowerbound = winUtil.getLowerBoundary(windowRef.constants);
				if(lowerbound == -1){ 
					// TODO manage error
				}
				
				List<TypeInformation<?>> typeClasses = new ArrayList<>();
				List<String> aggregators = new ArrayList<>();
				List<Integer> indexes = new ArrayList<>();
				for(RexWinAggCall agg: group.aggCalls){
					typeClasses.add(FlinkTypeFactory.toTypeInfo(agg.type));
					aggregators.add(agg.getKind().toString());
					indexes.add(((RexInputRef)agg.getOperands().get(0)).getIndex());
				}
				
				aggregateWindow =keyedS.
						countWindow(lowerbound,1)
						.apply(new DataStreamProcTimeAggregateRowWindowFunction(aggregators,
																				indexes,
																				typeClasses))
						.returns((TypeInformation<Object>)returnType);
			}
		} else {

		}

		return aggregateWindow;
	}

	@Override
	public String toString() {
		return super.toString() + "(" + "window=[" + windowRef + "]" + ")";
	}

	@Override
	public void explain(RelWriter pw) {
		super.explain(pw);
	}

//	@Override
//	public RelWriter explainTerms(RelWriter pw) {
//		for (Group group : window.groups) {
//			pw.item("Order", group.orderKeys.getFieldCollations());
//			pw.item("PartitionBy", group.keys);
//			pw.item("Time", "ProcTime()");
//			pw.item("LowBoundary", group.lowerBound);
//			pw.item("UpperBoundary", group.upperBound);
//		}
//		return pw;
//	}

}
