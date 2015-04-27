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

package org.apache.flink.optimizer.dataexchange;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.testfunctions.IdentityKeyExtractor;
import org.apache.flink.optimizer.testfunctions.Top1GroupReducer;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This test verifies that the optimizer assigns the correct
 * data exchange mode to a simple forward / shuffle plan.
 *
 * <pre>
 *     (source) -> (map) -> (filter) -> (groupBy / reduce)
 * </pre>
 */
@SuppressWarnings("serial")
public class DataExchangeModeForwardTest extends CompilerTestBase {


	@Test
	public void testPipelinedForced() {
		// PIPELINED_FORCED should result in pipelining all the way
		verifySimpleForwardPlan(ExecutionMode.PIPELINED_FORCED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED);
	}

	@Test
	public void testPipelined() {
		// PIPELINED should result in pipelining all the way
		verifySimpleForwardPlan(ExecutionMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED);
	}

	@Test
	public void testBatch() {
		// BATCH should result in batching the shuffle all the way
		verifySimpleForwardPlan(ExecutionMode.BATCH,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.BATCH, DataExchangeMode.PIPELINED);
	}

	@Test
	public void testBatchForced() {
		// BATCH_FORCED should result in batching all the way
		verifySimpleForwardPlan(ExecutionMode.BATCH_FORCED,
				DataExchangeMode.BATCH, DataExchangeMode.BATCH,
				DataExchangeMode.BATCH, DataExchangeMode.PIPELINED,
				DataExchangeMode.BATCH, DataExchangeMode.BATCH);
	}

	private void verifySimpleForwardPlan(ExecutionMode execMode,
										DataExchangeMode toMap,
										DataExchangeMode toFilter,
										DataExchangeMode toKeyExtractor,
										DataExchangeMode toCombiner,
										DataExchangeMode toReduce,
										DataExchangeMode toSink)
	{
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.getConfig().setExecutionMode(execMode);

			DataSet<String> dataSet = env.readTextFile("/never/accessed");
			dataSet
				.map(new MapFunction<String, Integer>() {
					@Override
					public Integer map(String value) {
						return 0;
					}
				})
				.filter(new FilterFunction<Integer>() {
					@Override
					public boolean filter(Integer value) {
						return false;
					}
				})
				.groupBy(new IdentityKeyExtractor<Integer>())
				.reduceGroup(new Top1GroupReducer<Integer>())
				.output(new DiscardingOutputFormat<Integer>());

			OptimizedPlan optPlan = compileNoStats(env.createProgramPlan());
			SinkPlanNode sinkNode = optPlan.getDataSinks().iterator().next();

			SingleInputPlanNode reduceNode = (SingleInputPlanNode) sinkNode.getPredecessor();
			SingleInputPlanNode combineNode = (SingleInputPlanNode) reduceNode.getPredecessor();
			SingleInputPlanNode keyExtractorNode = (SingleInputPlanNode) combineNode.getPredecessor();

			SingleInputPlanNode filterNode = (SingleInputPlanNode) keyExtractorNode.getPredecessor();
			SingleInputPlanNode mapNode = (SingleInputPlanNode) filterNode.getPredecessor();

			assertEquals(toMap, mapNode.getInput().getDataExchangeMode());
			assertEquals(toFilter, filterNode.getInput().getDataExchangeMode());
			assertEquals(toKeyExtractor, keyExtractorNode.getInput().getDataExchangeMode());
			assertEquals(toCombiner, combineNode.getInput().getDataExchangeMode());
			assertEquals(toReduce, reduceNode.getInput().getDataExchangeMode());
			assertEquals(toSink, sinkNode.getInput().getDataExchangeMode());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
