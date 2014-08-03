/**
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

package org.apache.flink.compiler;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plan.SingleInputPlanNode;
import org.apache.flink.compiler.plan.SinkPlanNode;
import org.apache.flink.compiler.plan.SourcePlanNode;

import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class GroupReduceCompilationTest extends CompilerTestBase implements java.io.Serializable {

	@Test
	public void testAllGroupReduceNoCombiner() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(8);
			
			DataSet<Double> data = env.fromElements(0.2, 0.3, 0.4, 0.5).name("source");
			
			data.reduceGroup(new RichGroupReduceFunction<Double, Double>() {
				public void reduce(Iterable<Double> values, Collector<Double> out) {}
			}).name("reducer")
			.print().name("sink");
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);
			
			
			// the all-reduce has no combiner, when the DOP of the input is one
			
			SourcePlanNode sourceNode = resolver.getNode("source");
			SingleInputPlanNode reduceNode = resolver.getNode("reducer");
			SinkPlanNode sinkNode = resolver.getNode("sink");
			
			// check wiring
			assertEquals(sourceNode, reduceNode.getInput().getSource());
			assertEquals(reduceNode, sinkNode.getInput().getSource());
			
			// check that reduce has the right strategy
			assertEquals(DriverStrategy.ALL_GROUP_REDUCE, reduceNode.getDriverStrategy());
			
			// check DOP
			assertEquals(1, sourceNode.getDegreeOfParallelism());
			assertEquals(1, reduceNode.getDegreeOfParallelism());
			assertEquals(1, sinkNode.getDegreeOfParallelism());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
		}
	}
	
	@Test
	public void testAllReduceWithCombiner() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(8);
			
			DataSet<Long> data = env.generateSequence(1, 8000000).name("source");
			
			GroupReduceOperator<Long, Long> reduced = data.reduceGroup(new RichGroupReduceFunction<Long, Long>() {
				public void reduce(Iterable<Long> values, Collector<Long> out) {}
			}).name("reducer");
			
			reduced.setCombinable(true);
			reduced.print().name("sink");
			
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
			assertEquals(DriverStrategy.ALL_GROUP_REDUCE, reduceNode.getDriverStrategy());
			assertEquals(DriverStrategy.ALL_GROUP_COMBINE, combineNode.getDriverStrategy());
			
			// check DOP
			assertEquals(8, sourceNode.getDegreeOfParallelism());
			assertEquals(8, combineNode.getDegreeOfParallelism());
			assertEquals(1, reduceNode.getDegreeOfParallelism());
			assertEquals(1, sinkNode.getDegreeOfParallelism());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
		}
	}
	
	
	@Test
	public void testGroupedReduceWithFieldPositionKeyNonCombinable() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(8);
			
			DataSet<Tuple2<String, Double>> data = env.readCsvFile("file:///will/never/be/read").types(String.class, Double.class)
				.name("source").setParallelism(6);
			
			data
				.groupBy(1)
				.reduceGroup(new RichGroupReduceFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
				public void reduce(Iterable<Tuple2<String, Double>> values, Collector<Tuple2<String, Double>> out) {}
			}).name("reducer")
			.print().name("sink");
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);
			
			// get the original nodes
			SourcePlanNode sourceNode = resolver.getNode("source");
			SingleInputPlanNode reduceNode = resolver.getNode("reducer");
			SinkPlanNode sinkNode = resolver.getNode("sink");
			
			// check wiring
			assertEquals(sourceNode, reduceNode.getInput().getSource());
			assertEquals(reduceNode, sinkNode.getInput().getSource());
			
			// check that both reduce and combiner have the same strategy
			assertEquals(DriverStrategy.SORTED_GROUP_REDUCE, reduceNode.getDriverStrategy());
			
			// check the keys
			assertEquals(new FieldList(1), reduceNode.getKeys());
			assertEquals(new FieldList(1), reduceNode.getInput().getLocalStrategyKeys());
			
			// check DOP
			assertEquals(6, sourceNode.getDegreeOfParallelism());
			assertEquals(8, reduceNode.getDegreeOfParallelism());
			assertEquals(8, sinkNode.getDegreeOfParallelism());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
		}
	}
	
	@Test
	public void testGroupedReduceWithFieldPositionKeyCombinable() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(8);
			
			DataSet<Tuple2<String, Double>> data = env.readCsvFile("file:///will/never/be/read").types(String.class, Double.class)
				.name("source").setParallelism(6);
			
			GroupReduceOperator<Tuple2<String, Double>, Tuple2<String, Double>> reduced = data
					.groupBy(1)
					.reduceGroup(new RichGroupReduceFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
				public void reduce(Iterable<Tuple2<String, Double>> values, Collector<Tuple2<String, Double>> out) {}
			}).name("reducer");
			
			reduced.setCombinable(true);
			reduced.print().name("sink");
			
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
			assertEquals(DriverStrategy.SORTED_GROUP_REDUCE, reduceNode.getDriverStrategy());
			assertEquals(DriverStrategy.SORTED_GROUP_COMBINE, combineNode.getDriverStrategy());
			
			// check the keys
			assertEquals(new FieldList(1), reduceNode.getKeys());
			assertEquals(new FieldList(1), combineNode.getKeys());
			assertEquals(new FieldList(1), reduceNode.getInput().getLocalStrategyKeys());
			
			// check DOP
			assertEquals(6, sourceNode.getDegreeOfParallelism());
			assertEquals(6, combineNode.getDegreeOfParallelism());
			assertEquals(8, reduceNode.getDegreeOfParallelism());
			assertEquals(8, sinkNode.getDegreeOfParallelism());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
		}
	}
	
	@Test
	public void testGroupedReduceWithSelectorFunctionKeyNoncombinable() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(8);
			
			DataSet<Tuple2<String, Double>> data = env.readCsvFile("file:///will/never/be/read").types(String.class, Double.class)
				.name("source").setParallelism(6);
			
			data
				.groupBy(new KeySelector<Tuple2<String,Double>, String>() { 
					public String getKey(Tuple2<String, Double> value) { return value.f0; }
				})
				.reduceGroup(new RichGroupReduceFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
				public void reduce(Iterable<Tuple2<String, Double>> values, Collector<Tuple2<String, Double>> out) {}
			}).name("reducer")
			.print().name("sink");
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);
			
			// get the original nodes
			SourcePlanNode sourceNode = resolver.getNode("source");
			SingleInputPlanNode reduceNode = resolver.getNode("reducer");
			SinkPlanNode sinkNode = resolver.getNode("sink");
			
			// get the key extractors and projectors
			SingleInputPlanNode keyExtractor = (SingleInputPlanNode) reduceNode.getInput().getSource();
			SingleInputPlanNode keyProjector = (SingleInputPlanNode) sinkNode.getInput().getSource();
			
			// check wiring
			assertEquals(sourceNode, keyExtractor.getInput().getSource());
			assertEquals(keyProjector, sinkNode.getInput().getSource());
			
			// check that both reduce and combiner have the same strategy
			assertEquals(DriverStrategy.SORTED_GROUP_REDUCE, reduceNode.getDriverStrategy());
			
			// check the keys
			assertEquals(new FieldList(0), reduceNode.getKeys());
			assertEquals(new FieldList(0), reduceNode.getInput().getLocalStrategyKeys());
			
			// check DOP
			assertEquals(6, sourceNode.getDegreeOfParallelism());
			assertEquals(6, keyExtractor.getDegreeOfParallelism());
			
			assertEquals(8, reduceNode.getDegreeOfParallelism());
			assertEquals(8, keyProjector.getDegreeOfParallelism());
			assertEquals(8, sinkNode.getDegreeOfParallelism());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
		}
	}
	
	@Test
	public void testGroupedReduceWithSelectorFunctionKeyCombinable() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(8);
			
			DataSet<Tuple2<String, Double>> data = env.readCsvFile("file:///will/never/be/read").types(String.class, Double.class)
				.name("source").setParallelism(6);
			
			GroupReduceOperator<Tuple2<String, Double>, Tuple2<String, Double>> reduced = data
				.groupBy(new KeySelector<Tuple2<String,Double>, String>() { 
					public String getKey(Tuple2<String, Double> value) { return value.f0; }
				})
				.reduceGroup(new RichGroupReduceFunction<Tuple2<String, Double>, Tuple2<String, Double>>() {
				public void reduce(Iterable<Tuple2<String, Double>> values, Collector<Tuple2<String, Double>> out) {}
			}).name("reducer");
			
			reduced.setCombinable(true);
			reduced.print().name("sink");
			
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
			assertEquals(DriverStrategy.SORTED_GROUP_REDUCE, reduceNode.getDriverStrategy());
			assertEquals(DriverStrategy.SORTED_GROUP_COMBINE, combineNode.getDriverStrategy());
			
			// check the keys
			assertEquals(new FieldList(0), reduceNode.getKeys());
			assertEquals(new FieldList(0), combineNode.getKeys());
			assertEquals(new FieldList(0), reduceNode.getInput().getLocalStrategyKeys());
			
			// check DOP
			assertEquals(6, sourceNode.getDegreeOfParallelism());
			assertEquals(6, keyExtractor.getDegreeOfParallelism());
			assertEquals(6, combineNode.getDegreeOfParallelism());
			
			assertEquals(8, reduceNode.getDegreeOfParallelism());
			assertEquals(8, keyProjector.getDegreeOfParallelism());
			assertEquals(8, sinkNode.getDegreeOfParallelism());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
		}
	}
}
