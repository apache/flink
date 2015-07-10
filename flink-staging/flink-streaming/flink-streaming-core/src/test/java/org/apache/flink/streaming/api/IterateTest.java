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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream.ConnectedIterativeDataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamLoop;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

@SuppressWarnings({ "unchecked", "unused", "serial" })
public class IterateTest extends StreamingMultipleProgramsTestBase {

	private static final long MEMORYSIZE = 32;
	private static boolean iterated[];
	private static int PARALLELISM = 2;

	@Test
	public void testException() throws Exception {

		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);
		DataStream<Integer> source = env.fromElements(1, 10);
		IterativeDataStream<Integer> iter1 = source.iterate();
		IterativeDataStream<Integer> iter2 = source.iterate();

		iter1.closeWith(iter1.map(NoOpIntMap));
		// Check for double closing
		try {
			iter1.closeWith(iter1.map(NoOpIntMap));
			fail();
		} catch (Exception e) {
		}

		// Check for closing iteration without head
		try {
			iter2.closeWith(iter1.map(NoOpIntMap));
			fail();
		} catch (Exception e) {
		}

		iter2.map(NoOpIntMap);

		// Check for executing with empty iteration
		try {
			env.execute();
			fail();
		} catch (Exception e) {
		}
	}

	@Test
	public void testImmutabilityWithCoiteration() {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);
		DataStream<Integer> source = env.fromElements(1, 10);

		IterativeDataStream<Integer> iter1 = source.iterate();
		// Calling withFeedbackType should create a new iteration
		ConnectedIterativeDataStream<Integer, String> iter2 = iter1.withFeedbackType(String.class);

		iter1.closeWith(iter1.map(NoOpIntMap));
		iter2.closeWith(iter2.map(NoOpCoMap));

		StreamGraph graph = env.getStreamGraph();

		graph.getJobGraph();

		assertEquals(2, graph.getStreamLoops().size());
		for (StreamLoop loop : graph.getStreamLoops()) {
			assertEquals(loop.getHeads(), loop.getTails());
			List<Tuple2<StreamNode, StreamNode>> sourceSinkPairs = loop.getSourceSinkPairs();
			assertEquals(1, sourceSinkPairs.size());
		}
	}

	@Test
	public void testmultipleHeadsTailsSimple() {
		StreamExecutionEnvironment env = new TestStreamEnvironment(4, MEMORYSIZE);
		DataStream<Integer> source1 = env.fromElements(1, 2, 3, 4, 5).shuffle();
		DataStream<Integer> source2 = env.fromElements(1, 2, 3, 4, 5);

		IterativeDataStream<Integer> iter1 = source1.union(source2).iterate();

		DataStream<Integer> head1 = iter1.map(NoOpIntMap);
		DataStream<Integer> head2 = iter1.map(NoOpIntMap).setParallelism(2);
		DataStream<Integer> head3 = iter1.map(NoOpIntMap).setParallelism(2)
				.addSink(new NoOpSink<Integer>());
		DataStream<Integer> head4 = iter1.map(NoOpIntMap).addSink(new NoOpSink<Integer>());

		SplitDataStream<Integer> source3 = env.fromElements(1, 2, 3, 4, 5).split(
				new OutputSelector<Integer>() {

					@Override
					public Iterable<String> select(Integer value) {
						return value % 2 == 0 ? Arrays.asList("even") : Arrays.asList("odd");
					}
				});

		iter1.closeWith(source3.select("even").union(
				head1.map(NoOpIntMap).broadcast().setParallelism(1), head2.shuffle()));

		StreamGraph graph = env.getStreamGraph();

		JobGraph jg = graph.getJobGraph();

		assertEquals(1, graph.getStreamLoops().size());
		StreamLoop loop = new ArrayList<StreamLoop>(graph.getStreamLoops()).get(0);

		assertEquals(4, loop.getHeads().size());
		assertEquals(3, loop.getTails().size());

		assertEquals(1, loop.getSourceSinkPairs().size());
		Tuple2<StreamNode, StreamNode> pair = loop.getSourceSinkPairs().get(0);

		assertEquals(pair.f0.getParallelism(), pair.f1.getParallelism());
		assertEquals(4, pair.f0.getOutEdges().size());
		assertEquals(3, pair.f1.getInEdges().size());

		for (StreamEdge edge : pair.f0.getOutEdges()) {
			assertTrue(edge.getPartitioner() instanceof ShufflePartitioner);
		}
		for (StreamEdge edge : pair.f1.getInEdges()) {
			assertTrue(edge.getPartitioner() instanceof RebalancePartitioner);
		}

		assertTrue(loop.getTailSelectedNames().contains(Arrays.asList("even")));

		// Test co-location

		JobVertex itSource1 = null;
		JobVertex itSink1 = null;

		for (JobVertex vertex : jg.getVertices()) {
			if (vertex.getName().contains("IterationSource")) {
				itSource1 = vertex;
			} else if (vertex.getName().contains("IterationSink")) {

				itSink1 = vertex;

			}
		}

		assertTrue(itSource1.getCoLocationGroup() != null);
		assertEquals(itSource1.getCoLocationGroup(), itSink1.getCoLocationGroup());
	}

	@Test
	public void testmultipleHeadsTailsWithTailPartitioning() {
		StreamExecutionEnvironment env = new TestStreamEnvironment(4, MEMORYSIZE);
		DataStream<Integer> source1 = env.fromElements(1, 2, 3, 4, 5).shuffle();
		DataStream<Integer> source2 = env.fromElements(1, 2, 3, 4, 5);

		IterativeDataStream<Integer> iter1 = source1.union(source2).iterate();

		DataStream<Integer> head1 = iter1.map(NoOpIntMap);
		DataStream<Integer> head2 = iter1.map(NoOpIntMap).setParallelism(2).name("shuffle");
		DataStream<Integer> head3 = iter1.map(NoOpIntMap).setParallelism(2)
				.addSink(new NoOpSink<Integer>());
		DataStream<Integer> head4 = iter1.map(NoOpIntMap).addSink(new NoOpSink<Integer>());

		SplitDataStream<Integer> source3 = env.fromElements(1, 2, 3, 4, 5).name("split")
				.split(new OutputSelector<Integer>() {

					@Override
					public Iterable<String> select(Integer value) {
						return value % 2 == 0 ? Arrays.asList("even") : Arrays.asList("odd");
					}
				});

		iter1.closeWith(
				source3.select("even").union(
						head1.map(NoOpIntMap).broadcast().setParallelism(1).name("bc"),
						head2.shuffle()), true);

		StreamGraph graph = env.getStreamGraph();

		JobGraph jg = graph.getJobGraph();

		assertEquals(1, graph.getStreamLoops().size());

		StreamLoop loop = new ArrayList<StreamLoop>(graph.getStreamLoops()).get(0);

		assertEquals(4, loop.getHeads().size());
		assertEquals(3, loop.getTails().size());

		assertEquals(2, loop.getSourceSinkPairs().size());
		List<Tuple2<StreamNode, StreamNode>> pairs = loop.getSourceSinkPairs();
		Tuple2<StreamNode, StreamNode> pair1 = pairs.get(0).f0.getParallelism() == 2 ? pairs.get(0)
				: pairs.get(1);
		Tuple2<StreamNode, StreamNode> pair2 = pairs.get(0).f0.getParallelism() == 4 ? pairs.get(0)
				: pairs.get(1);

		assertEquals(pair1.f0.getParallelism(), pair1.f1.getParallelism());
		assertEquals(2, pair1.f0.getParallelism());
		assertEquals(2, pair1.f0.getOutEdges().size());
		assertEquals(3, pair1.f1.getInEdges().size());

		for (StreamEdge edge : pair1.f0.getOutEdges()) {
			assertTrue(edge.getPartitioner() instanceof RebalancePartitioner);
			assertEquals(2, edge.getTargetVertex().getParallelism());
		}
		for (StreamEdge edge : pair1.f1.getInEdges()) {
			String tailName = edge.getSourceVertex().getOperatorName();
			if (tailName.equals("split")) {
				assertTrue(edge.getPartitioner() instanceof RebalancePartitioner);
			} else if (tailName.equals("bc")) {
				assertTrue(edge.getPartitioner() instanceof BroadcastPartitioner);
			} else if (tailName.equals("shuffle")) {
				assertTrue(edge.getPartitioner() instanceof ShufflePartitioner);
			}

		}

		assertEquals(pair2.f0.getParallelism(), pair2.f1.getParallelism());
		assertEquals(4, pair2.f0.getParallelism());
		assertEquals(2, pair2.f0.getOutEdges().size());
		assertEquals(3, pair2.f1.getInEdges().size());

		for (StreamEdge edge : pair2.f0.getOutEdges()) {
			assertTrue(edge.getPartitioner() instanceof RebalancePartitioner);
			assertEquals(4, edge.getTargetVertex().getParallelism());
		}
		for (StreamEdge edge : pair2.f1.getInEdges()) {
			String tailName = edge.getSourceVertex().getOperatorName();
			if (tailName.equals("split")) {
				assertTrue(edge.getPartitioner() instanceof RebalancePartitioner);
			} else if (tailName.equals("bc")) {
				assertTrue(edge.getPartitioner() instanceof BroadcastPartitioner);
			} else if (tailName.equals("shuffle")) {
				assertTrue(edge.getPartitioner() instanceof ShufflePartitioner);
			}

		}

		assertTrue(loop.getTailSelectedNames().contains(Arrays.asList("even")));

		// Test co-location

		JobVertex itSource1 = null;
		JobVertex itSource2 = null;
		JobVertex itSink1 = null;
		JobVertex itSink2 = null;

		for (JobVertex vertex : jg.getVertices()) {
			if (vertex.getName().contains("IterationSource")) {
				if (vertex.getName().contains("_0")) {
					itSource1 = vertex;
				} else if (vertex.getName().contains("_1")) {
					itSource2 = vertex;
				}
			} else if (vertex.getName().contains("IterationSink")) {
				if (vertex.getName().contains("_0")) {
					itSink1 = vertex;
				} else if (vertex.getName().contains("_1")) {
					itSink2 = vertex;
				}
			}
		}

		assertTrue(itSource1.getCoLocationGroup() != null);
		assertTrue(itSource2.getCoLocationGroup() != null);

		assertEquals(itSource1.getCoLocationGroup(), itSink1.getCoLocationGroup());
		assertEquals(itSource2.getCoLocationGroup(), itSink2.getCoLocationGroup());
		assertNotEquals(itSource1.getCoLocationGroup(), itSource2.getCoLocationGroup());
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSimpleIteration() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);
		iterated = new boolean[PARALLELISM];

		DataStream<Boolean> source = env
				.fromCollection(Collections.nCopies(PARALLELISM * 2, false));

		IterativeDataStream<Boolean> iteration = source.iterate(3000);

		DataStream<Boolean> increment = iteration.flatMap(new IterationHead()).map(NoOpBoolMap);

		iteration.map(NoOpBoolMap).addSink(new NoOpSink());

		iteration.closeWith(increment).addSink(new NoOpSink());

		env.execute();

		for (boolean iter : iterated) {
			assertTrue(iter);
		}

	}

	@Test
	public void testCoIteration() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		ConnectedIterativeDataStream<Integer, String> coIt = env.fromElements(0, 0).iterate(2000)
				.withFeedbackType("String");

		try {
			coIt.groupBy(1, 2);
			fail();
		} catch (UnsupportedOperationException e) {
		}

		DataStream<String> head = coIt
				.flatMap(new RichCoFlatMapFunction<Integer, String, String>() {

					private static final long serialVersionUID = 1L;
					boolean seenFromSource = false;

					@Override
					public void flatMap1(Integer value, Collector<String> out) throws Exception {
						out.collect(((Integer) (value + 1)).toString());
					}

					@Override
					public void flatMap2(String value, Collector<String> out) throws Exception {
						Integer intVal = Integer.valueOf(value);
						if (intVal < 2) {
							out.collect(((Integer) (intVal + 1)).toString());
						}
						if (intVal == 1000 || intVal == 2000) {
							seenFromSource = true;
						}
					}

					@Override
					public void close() {
						assertTrue(seenFromSource);
					}
				});

		coIt.map(new CoMapFunction<Integer, String, String>() {

			@Override
			public String map1(Integer value) throws Exception {
				return value.toString();
			}

			@Override
			public String map2(String value) throws Exception {
				return value;
			}
		}).setParallelism(1).addSink(new NoOpSink<String>());

		coIt.closeWith(head.broadcast().union(env.fromElements("1000", "2000").rebalance()));

		head.addSink(new TestSink()).setParallelism(1);

		env.execute();

		Collections.sort(TestSink.collected);
		assertEquals(Arrays.asList("1", "1", "2", "2", "2", "2"), TestSink.collected);
		assertEquals(2, new ArrayList<StreamLoop>(env.getStreamGraph().getStreamLoops()).get(0)
				.getSourceSinkPairs().size());

	}

	@Test
	public void testGroupByFeedback() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		KeySelector<Integer, Integer> key = new KeySelector<Integer, Integer>() {

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value % 3;
			}
		};

		DataStream<Integer> source = env.fromElements(1, 2, 3);

		IterativeDataStream<Integer> it = source.groupBy(key).iterate(3000);

		DataStream<Integer> head = it.flatMap(new RichFlatMapFunction<Integer, Integer>() {

			int received = 0;
			int key = -1;

			@Override
			public void flatMap(Integer value, Collector<Integer> out) throws Exception {
				received++;
				if (key == -1) {
					key = value % 3;
				} else {
					assertEquals(key, value % 3);
				}
				if (value > 0) {
					out.collect(value - 1);
				}
			}

			@Override
			public void close() {
				assertTrue(received > 1);
			}
		});

		it.closeWith(head.groupBy(key).union(head.map(NoOpIntMap).setParallelism(2).groupBy(key)),
				true).addSink(new NoOpSink<Integer>());

		env.execute();
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testWithCheckPointing() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);
		env.enableCheckpointing();

		DataStream<Boolean> source = env
				.fromCollection(Collections.nCopies(PARALLELISM * 2, false));

		IterativeDataStream<Boolean> iteration = source.iterate(3000);

		iteration.closeWith(iteration.flatMap(new IterationHead()));

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

	public static final class IterationHead extends RichFlatMapFunction<Boolean, Boolean> {
		public void flatMap(Boolean value, Collector<Boolean> out) throws Exception {
			int indx = getRuntimeContext().getIndexOfThisSubtask();
			if (value) {
				iterated[indx] = true;
			} else {
				out.collect(true);
			}
		}
	}

	public static final class NoOpSink<T> extends RichSinkFunction<T> {
		private List<T> received;

		public void invoke(T tuple) {
			received.add(tuple);
		}

		public void open(Configuration conf) {
			received = new ArrayList<T>();
		}

		public void close() {
			assertTrue(received.size() > 0);
		}
	}

	public static CoMapFunction<Integer, String, String> NoOpCoMap = new CoMapFunction<Integer, String, String>() {

		public String map1(Integer value) throws Exception {
			return value.toString();
		}

		public String map2(String value) throws Exception {
			return value;
		}
	};

	public static MapFunction<Integer, Integer> NoOpIntMap = new MapFunction<Integer, Integer>() {

		public Integer map(Integer value) throws Exception {
			return value;
		}

	};

	public static MapFunction<Boolean, Boolean> NoOpBoolMap = new MapFunction<Boolean, Boolean>() {

		public Boolean map(Boolean value) throws Exception {
			return value;
		}

	};

	public static class TestSink implements SinkFunction<String> {

		private static final long serialVersionUID = 1L;
		public static List<String> collected = new ArrayList<String>();

		@Override
		public void invoke(String value) throws Exception {
			collected.add(value);
		}

	}

}
