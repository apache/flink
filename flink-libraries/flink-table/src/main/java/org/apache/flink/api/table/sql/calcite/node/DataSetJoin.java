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
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.join.JoinType;
import org.apache.flink.api.table.sql.calcite.DataSetRelNode;

/**
 * Flink RelNode which matches along with JoinOperator and its related operations.
 */
public class DataSetJoin<L, R, OUT> extends BiRel implements DataSetRelNode<OUT> {
	
	public DataSetJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right) {
		super(cluster, traitSet, left, right);
	}
	
	private TypeInformation<L> getLeftInputType() {
		return null;
	}
	
	private TypeInformation<R> getRightInputType() {
		return null;
	}
	
	private TypeInformation<OUT> getType() {
		return null;
	}
	
	private String getName() {
		return null;
	}
	
	private JoinType getJoinType() {
		return null;
	}
	
	private JoinHint getJoinHint() {
		return null;
	}
	
	private int[] getLeftJoinKey() {
		return null;
	}
	
	private int[] getRightJoinKey() {
		return null;
	}
	
	private JoinFunction<L, R, OUT> getJoinFunction() {
		return null;
	}
	
	@Override
	public DataSet<OUT> translateToPlan() {
		return null;
	}
}
