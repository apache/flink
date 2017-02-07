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
package org.apache.flink.table.plan.logical.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.plan.logical.rel.util.StreamGroupKeySelector;
import org.apache.flink.table.plan.logical.rel.util.WindowAggregateUtil;
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel;
import org.apache.flink.table.plan.nodes.datastream.DataStreamRelJava;
import org.apache.flink.table.typeutils.TypeConverter;

import scala.Option;

public class DataStreamProcTimeRowAggregate extends DataStreamRelJava {

	LogicalWindow window;
	RelTraitSet traitSet;
	RelNode input;
	RelDataType rowType;
	String description;
	WindowAggregateUtil winUtil;

	public DataStreamProcTimeRowAggregate(
			RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode input,
			RelDataType rowType,
			String description,
			LogicalWindow window) {
		super(cluster, traitSet, input);
		this.window = window;
		this.rowType = rowType;
		this.description = description;
		this.winUtil = new WindowAggregateUtil();
	}

	@Override
	protected RelDataType deriveRowType() {
		return super.deriveRowType();
	}

	@Override
	public RelNode copy(
			RelTraitSet traitSet,
			java.util.List<RelNode> inputs) {
		return new DataStreamProcTimeRowAggregate(
				super.getCluster(),
				traitSet,
				input, 
				rowType, 
				description, 
				window);
	}

	@Override
	public DataStream<Object> translateToPlan(
			StreamTableEnvironment tableEnv,
			Option<TypeInformation<Object>> expectedType,
			Object ignore) {

		TableConfig config = tableEnv.getConfig();

		DataStream<Object> inputDS = ((DataStreamRel) input).translateToPlan(
															tableEnv,
															expectedType);
		
		System.out.println(inputDS);

		TypeInformation<?> returnType = TypeConverter.determineReturnType(
				getRowType(), 
				expectedType,
				config.getNullCheck(), 
				config.getEfficientTypeUsage());

		System.out.println(returnType);

		// TODO check type and return type consistency

		KeyedStream<Object, Tuple> keyedS = null;
		// apply partitions
		if (winUtil.isStreamPartitioned(window)) {
			for (final Group group : window.groups) {
				keyedS = inputDS.keyBy(new StreamGroupKeySelector(group));
			}
			keyedS.window(null);
		} else{
			
		}
		

		return null;
	}

	@Override
	public String toString() {
		return super.toString() + "(" + "window=[" + window + "]" + ")";
	}

	@Override
	public void explain(RelWriter pw) {
		super.explain(pw);
	}

	@Override
	public RelWriter explainTerms(RelWriter pw) {
		/*
		 * pw.item("Type", winConf.type); pw.item("Order",
		 * winConf.operateField); pw.item("PartitionBy", winConf.partitionBy);
		 * pw.itemIf("Event-based", winConf.eventWindow, winConf.eventWindow);
		 * pw.itemIf("Time-based", winConf.timeWindow, winConf.timeWindow);
		 * pw.item("LowBoundary", winConf.referenceLowBoundary);
		 * pw.itemIf("LowBoundary constant", winConf.lowBoudary,
		 * winConf.lowBoudary != 0); pw.item("HighBoundary",
		 * winConf.referenceHighBoundary); pw.itemIf("HighBoundary constant",
		 * winConf.highBoudary, winConf.highBoudary != 0);
		 */
		return pw;
	}

}

