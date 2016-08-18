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
package org.apache.flink.optimizer;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


@SuppressWarnings("serial")
public class DistinctCompilationTest extends CompilerTestBase implements java.io.Serializable {

	@Test
	public void testDistinctPlain() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(8);

			DataSet<Tuple2<String, Double>> data = env.readCsvFile("file:///will/never/be/read").types(String.class, Double.class)
					.name("source").setParallelism(6);

			data
					.distinct().name("reducer")
					.output(new DiscardingOutputFormat<Tuple2<String, Double>>()).name("sink");

			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);

			OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);

			// get the original nodes
			SourcePlanNode sourceNode = resolver.getNode("source");
			SingleInputPlanNode reduceNode = resolver.getNode("reducer");
			SinkPlanNode sinkNode = resolver.getNode("sink");

			// get the combiner
			SingleInputPlanNode combineNode = (SingleInputPlanNode) reduceNode.getInput().getSource();

			// check wiring
			assertEquals(sourceNode, combineNode.getInput().getSource());
			assertEquals(reduceNode, sinkNode.getInput().getSource());

			// check that both reduce and combiner have the same strategy
			assertEquals(DriverStrategy.SORTED_REDUCE, reduceNode.getDriverStrategy());
			assertEquals(DriverStrategy.SORTED_PARTIAL_REDUCE, combineNode.getDriverStrategy());

			// check the keys
			assertEquals(new FieldList(0, 1), reduceNode.getKeys(0));
			assertEquals(new FieldList(0, 1), combineNode.getKeys(0));
			assertEquals(new FieldList(0, 1), reduceNode.getInput().getLocalStrategyKeys());

			// check parallelism
			assertEquals(6, sourceNode.getParallelism());
			assertEquals(6, combineNode.getParallelism());
			assertEquals(8, reduceNode.getParallelism());
			assertEquals(8, sinkNode.getParallelism());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
		}
	}

	@Test
	public void testDistinctWithCombineHint() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(8);

			DataSet<Tuple2<String, Double>> data = env.readCsvFile("file:///will/never/be/read").types(String.class, Double.class)
					.name("source").setParallelism(6);

			data
					.distinct().setCombineHint(CombineHint.HASH).name("reducer")
					.output(new DiscardingOutputFormat<Tuple2<String, Double>>()).name("sink");

			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);

			OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);

			// get the original nodes
			SourcePlanNode sourceNode = resolver.getNode("source");
			SingleInputPlanNode reduceNode = resolver.getNode("reducer");
			SinkPlanNode sinkNode = resolver.getNode("sink");

			// get the combiner
			SingleInputPlanNode combineNode = (SingleInputPlanNode) reduceNode.getInput().getSource();

			// check wiring
			assertEquals(sourceNode, combineNode.getInput().getSource());
			assertEquals(reduceNode, sinkNode.getInput().getSource());

			// check that both reduce and combiner have the same strategy
			assertEquals(DriverStrategy.SORTED_REDUCE, reduceNode.getDriverStrategy());
			assertEquals(DriverStrategy.HASHED_PARTIAL_REDUCE, combineNode.getDriverStrategy());

			// check the keys
			assertEquals(new FieldList(0, 1), reduceNode.getKeys(0));
			assertEquals(new FieldList(0, 1), combineNode.getKeys(0));
			assertEquals(new FieldList(0, 1), reduceNode.getInput().getLocalStrategyKeys());

			// check parallelism
			assertEquals(6, sourceNode.getParallelism());
			assertEquals(6, combineNode.getParallelism());
			assertEquals(8, reduceNode.getParallelism());
			assertEquals(8, sinkNode.getParallelism());
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
		}
	}

	@Test
	public void testDistinctWithSelectorFunctionKey() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(8);

			DataSet<Tuple2<String, Double>> data = env.readCsvFile("file:///will/never/be/read").types(String.class, Double.class)
					.name("source").setParallelism(6);

			data
					.distinct(new KeySelector<Tuple2<String,Double>, String>() {
						public String getKey(Tuple2<String, Double> value) { return value.f0; }
					}).name("reducer")
					.output(new DiscardingOutputFormat<Tuple2<String, Double>>()).name("sink");

			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);

			OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);

			// get the original nodes
			SourcePlanNode sourceNode = resolver.getNode("source");
			SingleInputPlanNode reduceNode = resolver.getNode("reducer");
			SinkPlanNode sinkNode = resolver.getNode("sink");

			// get the combiner
			SingleInputPlanNode combineNode = (SingleInputPlanNode) reduceNode.getInput().getSource();

			// get the key extractors and projectors
			SingleInputPlanNode keyExtractor = (SingleInputPlanNode) combineNode.getInput().getSource();
			SingleInputPlanNode keyProjector = (SingleInputPlanNode) sinkNode.getInput().getSource();

			// check wiring
			assertEquals(sourceNode, keyExtractor.getInput().getSource());
			assertEquals(keyProjector, sinkNode.getInput().getSource());

			// check that both reduce and combiner have the same strategy
			assertEquals(DriverStrategy.SORTED_REDUCE, reduceNode.getDriverStrategy());
			assertEquals(DriverStrategy.SORTED_PARTIAL_REDUCE, combineNode.getDriverStrategy());

			// check the keys
			assertEquals(new FieldList(0), reduceNode.getKeys(0));
			assertEquals(new FieldList(0), combineNode.getKeys(0));
			assertEquals(new FieldList(0), reduceNode.getInput().getLocalStrategyKeys());

			// check parallelism
			assertEquals(6, sourceNode.getParallelism());
			assertEquals(6, keyExtractor.getParallelism());
			assertEquals(6, combineNode.getParallelism());

			assertEquals(8, reduceNode.getParallelism());
			assertEquals(8, keyProjector.getParallelism());
			assertEquals(8, sinkNode.getParallelism());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
		}
	}

	@Test
	public void testDistinctWithFieldPositionKeyCombinable() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(8);

			DataSet<Tuple2<String, Double>> data = env.readCsvFile("file:///will/never/be/read").types(String.class, Double.class)
					.name("source").setParallelism(6);

			DistinctOperator<Tuple2<String, Double>> reduced = data
					.distinct(1).name("reducer");

			reduced.output(new DiscardingOutputFormat<Tuple2<String, Double>>()).name("sink");

			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);

			OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);

			// get the original nodes
			SourcePlanNode sourceNode = resolver.getNode("source");
			SingleInputPlanNode reduceNode = resolver.getNode("reducer");
			SinkPlanNode sinkNode = resolver.getNode("sink");

			// get the combiner
			SingleInputPlanNode combineNode = (SingleInputPlanNode) reduceNode.getInput().getSource();

			// check wiring
			assertEquals(sourceNode, combineNode.getInput().getSource());
			assertEquals(reduceNode, sinkNode.getInput().getSource());

			// check that both reduce and combiner have the same strategy
			assertEquals(DriverStrategy.SORTED_REDUCE, reduceNode.getDriverStrategy());
			assertEquals(DriverStrategy.SORTED_PARTIAL_REDUCE, combineNode.getDriverStrategy());

			// check the keys
			assertEquals(new FieldList(1), reduceNode.getKeys(0));
			assertEquals(new FieldList(1), combineNode.getKeys(0));
			assertEquals(new FieldList(1), reduceNode.getInput().getLocalStrategyKeys());

			// check parallelism
			assertEquals(6, sourceNode.getParallelism());
			assertEquals(6, combineNode.getParallelism());
			assertEquals(8, reduceNode.getParallelism());
			assertEquals(8, sinkNode.getParallelism());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
		}
	}
}
