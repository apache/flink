/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraph.StreamLoop;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class IterateTest {

	private static final long MEMORYSIZE = 32;
	private static boolean iterated[];
	private static int PARALLELISM = 2;

	public static final class IterationHead extends RichFlatMapFunction<Boolean, Boolean> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Boolean value, Collector<Boolean> out) throws Exception {
			int indx = getRuntimeContext().getIndexOfThisSubtask();
			if (value) {
				iterated[indx] = true;
			} else {
				out.collect(value);
			}

		}

	}

	public static final class IterationTail extends RichFlatMapFunction<Boolean, Boolean> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Boolean value, Collector<Boolean> out) throws Exception {
			out.collect(true);

		}

	}

	public static final class MySink implements SinkFunction<Boolean> {

		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Boolean tuple) {
		}
	}

	public static final class NoOpMap implements MapFunction<Boolean, Boolean> {

		private static final long serialVersionUID = 1L;

		@Override
		public Boolean map(Boolean value) throws Exception {
			return value;
		}

	}

	public StreamExecutionEnvironment constructIterativeJob(StreamExecutionEnvironment env) {
		env.setBufferTimeout(10);

		DataStream<Boolean> source = env.fromCollection(Collections.nCopies(PARALLELISM, false));

		IterativeDataStream<Boolean> iteration = source.iterate(3000);

		DataStream<Boolean> increment = iteration.flatMap(new IterationHead()).flatMap(
				new IterationTail());

		iteration.closeWith(increment).addSink(new MySink());
		return env;
	}

	@Test
	public void testColocation() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(4, MEMORYSIZE);

		IterativeDataStream<Boolean> it = env.fromElements(true).rebalance().map(new NoOpMap())
				.iterate();

		DataStream<Boolean> head = it.map(new NoOpMap()).setParallelism(2).name("HeadOperator");

		it.closeWith(head.map(new NoOpMap()).setParallelism(3).name("TailOperator")).print();

		JobGraph graph = env.getStreamGraph().getJobGraph();

		JobVertex itSource = null;
		JobVertex itSink = null;
		JobVertex headOp = null;
		JobVertex tailOp = null;

		for (JobVertex vertex : graph.getVertices()) {
			if (vertex.getName().contains("IterationSource")) {
				itSource = vertex;
			} else if (vertex.getName().contains("IterationSink")) {
				itSink = vertex;
			} else if (vertex.getName().contains("HeadOperator")) {
				headOp = vertex;
			} else if (vertex.getName().contains("TailOp")) {
				tailOp = vertex;
			}
		}

		assertTrue(itSource.getCoLocationGroup() != null);
		assertEquals(itSource.getCoLocationGroup(), itSink.getCoLocationGroup());
		assertEquals(headOp.getParallelism(), 2);
		assertEquals(tailOp.getParallelism(), 3);
		assertEquals(itSource.getParallelism(), itSink.getParallelism());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testPartitioning() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(4, MEMORYSIZE);

		IterativeDataStream<Boolean> it = env.fromElements(true).iterate();

		IterativeDataStream<Boolean> it2 = env.fromElements(true).iterate();

		DataStream<Boolean> head = it.map(new NoOpMap()).name("Head1").broadcast();
		DataStream<Boolean> head2 = it2.map(new NoOpMap()).name("Head2").broadcast();

		it.closeWith(head.union(head.map(new NoOpMap()).shuffle()), true);
		it2.closeWith(head2, false);

		System.out.println(env.getExecutionPlan());
		StreamGraph graph = env.getStreamGraph();

		for (StreamLoop loop : graph.getStreamLoops()) {
			StreamEdge tailToSink = loop.getSink().getInEdges().get(0);
			if (tailToSink.getSourceVertex().getOperatorName().contains("Head1")) {
				assertTrue(tailToSink.getPartitioner() instanceof BroadcastPartitioner);
				assertTrue(loop.getSink().getInEdges().get(1).getPartitioner() instanceof ShufflePartitioner);
			} else {
				assertTrue(tailToSink.getPartitioner() instanceof RebalancePartitioner);
			}
		}

	}

	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);
		iterated = new boolean[PARALLELISM];

		env = constructIterativeJob(env);

		env.execute();

		for (boolean iter : iterated) {
			assertTrue(iter);
		}

	}

	@Test
	public void testWithCheckPointing() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);

		env = constructIterativeJob(env);

		env.enableCheckpointing();
		try {
			env.execute();

			// this statement should never be reached
			fail();
		} catch (UnsupportedOperationException e) {
			// expected behaviour
		}
		
		
		// Test force checkpointing

		try {
			env.enableCheckpointing(1, false);
			env.execute();

			// this statement should never be reached
			fail();
		} catch (UnsupportedOperationException e) {
			// expected behaviour
		}
		
		env.enableCheckpointing(1, true);
		env.getStreamGraph().getJobGraph();

	}

}
