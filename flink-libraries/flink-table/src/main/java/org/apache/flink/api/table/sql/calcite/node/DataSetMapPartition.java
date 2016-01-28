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
package org.apache.flink.api.table.sql.calcite.node;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.table.sql.calcite.DataSetRelNode;
import org.apache.flink.api.table.sql.calcite.DataSetRelNode;
import org.apache.flink.api.table.sql.calcite.DataSetRelNode;

public class DataSetMapPartition<IN, OUT> extends SingleRel implements DataSetRelNode<OUT> {
	
	private final static String NAME = "DataSetMapPartition";
	private TypeInformation<OUT> outputType;
	private MapPartitionFunction<IN, OUT> mapPartitionFunction;
	
	public DataSetMapPartition(RelOptCluster cluster, RelTraitSet traits, RelNode input,
		TypeInformation<OUT> outputType, MapPartitionFunction<IN, OUT> mapPartitionFunction) {
		
		super(cluster, traits, input);
		this.outputType = outputType;
		this.mapPartitionFunction = mapPartitionFunction;
	}
	
	private MapPartitionFunction<IN, OUT> getMapPartitionFunction() {
		return this.mapPartitionFunction;
	}
	
	private String getName() {
		return NAME;
	}
	
	private TypeInformation<OUT> getType() {
		return this.outputType;
	}
	
	@Override
	public DataSet<OUT> translateToPlan() {
		return null;
	}
}
