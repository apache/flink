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

package org.apache.flink.optimizer.plantranslate;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertTrue;

public class JobGraphGeneratorTest {

	/**
	 * Verifies that the resources are merged correctly for chained operators when
	 * generating job graph
	 */
	@Test
	public void testResourcesForChainedOperators() throws Exception {
		ResourceSpec resource1 = new ResourceSpec(0.1, 100);
		ResourceSpec resource2 = new ResourceSpec(0.2, 200);
		ResourceSpec resource3 = new ResourceSpec(0.3, 300);
		ResourceSpec resource4 = new ResourceSpec(0.4, 400);
		ResourceSpec resource5 = new ResourceSpec(0.5, 500);
		ResourceSpec resource6 = new ResourceSpec(0.6, 600);
		ResourceSpec resource7 = new ResourceSpec(0.7, 700);

		Method opMethod = Operator.class.getDeclaredMethod("setResources", ResourceSpec.class);
		opMethod.setAccessible(true);

		Method sinkMethod = DataSink.class.getDeclaredMethod("setResources", ResourceSpec.class);
		sinkMethod.setAccessible(true);

		MapFunction<Long, Long> mapFunction = new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				return value;
			}
		};

		FilterFunction<Long> filterFunction = new FilterFunction<Long>() {
			@Override
			public boolean filter(Long value) throws Exception {
				return false;
			}
		};

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Long> input = env.fromElements(1L, 2L, 3L);
		opMethod.invoke(input, resource1);

		DataSet<Long> map1 = input.map(mapFunction);
		opMethod.invoke(map1, resource2);

		// CHAIN(Source -> Map -> Filter)
		DataSet<Long> filter1 = map1.filter(filterFunction);
		opMethod.invoke(filter1, resource3);

		IterativeDataSet<Long> startOfIteration = filter1.iterate(10);
		opMethod.invoke(startOfIteration, resource4);

		DataSet<Long> map2 = startOfIteration.map(mapFunction);
		opMethod.invoke(map2, resource5);

		// CHAIN(Map -> Filter)
		DataSet<Long> feedback = map2.filter(filterFunction);
		opMethod.invoke(feedback, resource6);

		DataSink<Long> sink = startOfIteration.closeWith(feedback).output(new DiscardingOutputFormat<Long>());
		sinkMethod.invoke(sink, resource7);

		Plan plan = env.createProgramPlan();
		Optimizer pc = new Optimizer(new Configuration());
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		JobGraph jobGraph = jgg.compileJobGraph(op);

		JobVertex sourceMapFilterVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
		JobVertex iterationHeadVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
		JobVertex feedbackVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(2);
		JobVertex sinkVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(3);
		JobVertex iterationSyncVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(4);

		assertTrue(sourceMapFilterVertex.getMinResources().equals(resource1.merge(resource2).merge(resource3)));
		assertTrue(iterationHeadVertex.getPreferredResources().equals(resource4));
		assertTrue(feedbackVertex.getMinResources().equals(resource5.merge(resource6)));
		assertTrue(sinkVertex.getPreferredResources().equals(resource7));
		assertTrue(iterationSyncVertex.getMinResources().equals(resource4));
	}

	/**
	 * Verifies that the resources are set onto each job vertex correctly when generating job graph
	 * which covers the delta iteration case
	 */
	@Test
	public void testResourcesForDeltaIteration() throws Exception{
		ResourceSpec resource1 = new ResourceSpec(0.1, 100);
		ResourceSpec resource2 = new ResourceSpec(0.2, 200);
		ResourceSpec resource3 = new ResourceSpec(0.3, 300);
		ResourceSpec resource4 = new ResourceSpec(0.4, 400);
		ResourceSpec resource5 = new ResourceSpec(0.5, 500);
		ResourceSpec resource6 = new ResourceSpec(0.6, 600);

		Method opMethod = Operator.class.getDeclaredMethod("setResources", ResourceSpec.class);
		opMethod.setAccessible(true);

		Method deltaMethod = DeltaIteration.class.getDeclaredMethod("setResources", ResourceSpec.class);
		deltaMethod.setAccessible(true);

		Method sinkMethod = DataSink.class.getDeclaredMethod("setResources", ResourceSpec.class);
		sinkMethod.setAccessible(true);

		MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> mapFunction = new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
			@Override
			public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
				return value;
			}
		};

		FilterFunction<Tuple2<Long, Long>> filterFunction = new FilterFunction<Tuple2<Long, Long>>() {
			@Override
			public boolean filter(Tuple2<Long, Long> value) throws Exception {
				return false;
			}
		};

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> input = env.fromElements(new Tuple2<>(1L, 2L));
		opMethod.invoke(input, resource1);

		// CHAIN(Map -> Filter)
		DataSet<Tuple2<Long, Long>> map = input.map(mapFunction);
		opMethod.invoke(map, resource2);

		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = map.iterateDelta(map, 100, 0).
				registerAggregator("test", new LongSumAggregator());
		deltaMethod.invoke(iteration, resource3);

		DataSet<Tuple2<Long, Long>> delta = iteration.getWorkset().map(mapFunction);
		opMethod.invoke(delta, resource4);

		DataSet<Tuple2<Long, Long>> feedback = delta.filter(filterFunction);
		opMethod.invoke(feedback, resource5);

		DataSink<Tuple2<Long, Long>> sink = iteration.closeWith(delta, feedback).
				output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
		sinkMethod.invoke(sink, resource6);

		Plan plan = env.createProgramPlan();
		Optimizer pc = new Optimizer(new Configuration());
		OptimizedPlan op = pc.compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		JobGraph jobGraph = jgg.compileJobGraph(op);

		JobVertex sourceMapVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
		JobVertex iterationHeadVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
		JobVertex deltaVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(2);
		JobVertex iterationTailVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(3);
		JobVertex feedbackVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(4);
		JobVertex sinkVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(5);
		JobVertex iterationSyncVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(6);

		assertTrue(sourceMapVertex.getMinResources().equals(resource1.merge(resource2)));
		assertTrue(iterationHeadVertex.getPreferredResources().equals(resource3));
		assertTrue(deltaVertex.getMinResources().equals(resource4));
		// the iteration tail task will be scheduled in the same instance with iteration head, and currently not set resources.
		assertTrue(iterationTailVertex.getPreferredResources().equals(ResourceSpec.DEFAULT));
		assertTrue(feedbackVertex.getMinResources().equals(resource5));
		assertTrue(sinkVertex.getPreferredResources().equals(resource6));
		assertTrue(iterationSyncVertex.getMinResources().equals(resource3));
	}
}
