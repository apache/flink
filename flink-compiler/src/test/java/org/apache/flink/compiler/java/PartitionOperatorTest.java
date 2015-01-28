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

package org.apache.flink.compiler.java;

import static org.junit.Assert.*;

import java.util.Collections;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.compiler.CompilerTestBase;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plan.SingleInputPlanNode;
import org.apache.flink.compiler.plan.SinkPlanNode;
import org.apache.flink.compiler.testfunctions.IdentityGroupReducer;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.junit.Test;

@SuppressWarnings("serial")
public class PartitionOperatorTest extends CompilerTestBase {

	@Test
	public void testPartitionOperatorPreservesFields() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Tuple2<Long, Long>> data = env.fromCollection(Collections.singleton(new Tuple2<Long, Long>(0L, 0L)));
			
			data.partitionCustom(new Partitioner<Long>() {
					public int partition(Long key, int numPartitions) { return key.intValue(); }
				}, 1)
				.groupBy(1)
				.reduceGroup(new IdentityGroupReducer<Tuple2<Long,Long>>())
				.print();
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			SinkPlanNode sink = op.getDataSinks().iterator().next();
			SingleInputPlanNode reducer = (SingleInputPlanNode) sink.getInput().getSource();
			SingleInputPlanNode partitioner = (SingleInputPlanNode) reducer.getInput().getSource();

			assertEquals(ShipStrategyType.FORWARD, reducer.getInput().getShipStrategy());
			assertEquals(ShipStrategyType.PARTITION_CUSTOM, partitioner.getInput().getShipStrategy());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
