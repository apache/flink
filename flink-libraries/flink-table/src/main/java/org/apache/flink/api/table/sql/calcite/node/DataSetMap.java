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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.table.sql.calcite.DataSetRelNode;
import org.apache.flink.api.table.sql.calcite.DataSetRelNode;
import org.apache.flink.api.table.sql.calcite.DataSetRelNode;

/**
 * Flink RelNode which matches along with MapOperator.
 */
public class DataSetMap<IN, OUT> extends SingleRel implements DataSetRelNode<OUT> {
	
	private final static String NAME = "DataSetMap";
	private TypeInformation<OUT> outputType;
	private RichMapFunction<IN, OUT> mapFunction;
	
	public DataSetMap(RelOptCluster cluster, RelTraitSet traits, RelNode input, 
		TypeInformation<OUT> outputType, RichMapFunction<IN, OUT> mapFunction) {
		super(cluster, traits, input);
		this.outputType = outputType;
		this.mapFunction = mapFunction;
	}
	
	private TypeInformation<OUT> getType() {
		return this.outputType;
	}
	
	private String getName() {
		return NAME;
	}
	
	private RichMapFunction<IN, OUT> getMapFunction() {
		return this.mapFunction;
	}
	
	@Override
	public DataSet<OUT> translateToPlan() {
		return null;
	}
}
