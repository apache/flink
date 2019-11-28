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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.IterativeStream.ConnectedIterativeStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.test.streaming.runtime.util.EvenOddOutputSelector;
import org.apache.flink.test.streaming.runtime.util.NoOpIntMap;
import org.apache.flink.test.streaming.runtime.util.ReceiveCheckNoOpSink;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for streaming iterations.
 */
@SuppressWarnings({ "unchecked", "unused", "serial" })
public class IterateITCase extends AbstractTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(IterateITCase.class);

	private static boolean[] iterated;

	private int parallelism = miniClusterResource.getNumberSlots();

	@Test(expected = UnsupportedOperationException.class)
	public void testIncorrectParallelism() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source = env.fromElements(1, 10);

		IterativeStream<Integer> iter1 = source.iterate();
		SingleOutputStreamOperator<Integer> map1 = iter1.map(noOpIntMap);
		iter1.closeWith(map1).print();
	}

	@Test
	public void testDoubleClosing() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// introduce dummy mapper to get to correct parallelism
		DataStream<Integer> source = env.fromElements(1, 10).map(noOpIntMap);

		IterativeStream<Integer> iter1 = source.iterate();

		iter1.closeWith(iter1.map(noOpIntMap));
		iter1.closeWith(iter1.map(noOpIntMap));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testDifferingParallelism() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// introduce dummy mapper to get to correct parallelism
		DataStream<Integer> source = env.fromElements(1, 10)
				.map(noOpIntMap);

		IterativeStream<Integer> iter1 = source.iterate();

		iter1.closeWith(iter1.map(noOpIntMap).setParallelism(parallelism / 2));

	}

	@Test(expected = UnsupportedOperationException.class)
	public void testCoDifferingParallelism() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// introduce dummy mapper to get to correct parallelism
		DataStream<Integer> source = env.fromElements(1, 10).map(noOpIntMap);

		ConnectedIterativeStreams<Integer, Integer> coIter = source.iterate().withFeedbackType(
				Integer.class);

		coIter.closeWith(coIter.map(noOpIntCoMap).setParallelism(parallelism / 2));

	}

	@Test(expected = UnsupportedOperationException.class)
	public void testClosingFromOutOfLoop() throws Exception {

		// this test verifies that we cannot close an iteration with a DataStream that does not
		// have the iteration in its predecessors

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// introduce dummy mapper to get to correct parallelism
		DataStream<Integer> source = env.fromElements(1, 10).map(noOpIntMap);

		IterativeStream<Integer> iter1 = source.iterate();
		IterativeStream<Integer> iter2 = source.iterate();

		iter2.closeWith(iter1.map(noOpIntMap));

	}

	@Test(expected = UnsupportedOperationException.class)
	public void testCoIterClosingFromOutOfLoop() throws Exception {

		// this test verifies that we cannot close an iteration with a DataStream that does not
		// have the iteration in its predecessors

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// introduce dummy mapper to get to correct parallelism
		DataStream<Integer> source = env.fromElements(1, 10).map(noOpIntMap);

		IterativeStream<Integer> iter1 = source.iterate();
		ConnectedIterativeStreams<Integer, Integer> coIter = source.iterate().withFeedbackType(
				Integer.class);

		coIter.closeWith(iter1.map(noOpIntMap));

	}

	@Test(expected = IllegalStateException.class)
	public void testExecutionWithEmptyIteration() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source = env.fromElements(1, 10).map(noOpIntMap);

		IterativeStream<Integer> iter1 = source.iterate();

		iter1.map(noOpIntMap).print();

		env.execute();
	}

	@Test
	public void testImmutabilityWithCoiteration() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source = env.fromElements(1, 10).map(noOpIntMap); // for rebalance

		IterativeStream<Integer> iter1 = source.iterate();
		// Calling withFeedbackType should create a new iteration
		ConnectedIterativeStreams<Integer, String> iter2 = iter1.withFeedbackType(String.class);

		iter1.closeWith(iter1.map(noOpIntMap)).print();
		iter2.closeWith(iter2.map(noOpCoMap)).print();

		StreamGraph graph = env.getStreamGraph();

		assertEquals(2, graph.getIterationSourceSinkPairs().size());

		for (Tuple2<StreamNode, StreamNode> sourceSinkPair: graph.getIterationSourceSinkPairs()) {
			assertEquals(graph.getTargetVertex(sourceSinkPair.f0.getOutEdges().get(0)),
				graph.getSourceVertex(sourceSinkPair.f1.getInEdges().get(0)));
		}
	}

	@Test
	public void testmultipleHeadsTailsSimple() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source1 = env.fromElements(1, 2, 3, 4, 5)
				.shuffle()
				.map(noOpIntMap).name("ParallelizeMapShuffle");
		DataStream<Integer> source2 = env.fromElements(1, 2, 3, 4, 5)
				.map(noOpIntMap).name("ParallelizeMapRebalance");

		IterativeStream<Integer> iter1 = source1.union(source2).iterate();

		DataStream<Integer> head1 = iter1.map(noOpIntMap).name("IterRebalanceMap").setParallelism(parallelism / 2);
		DataStream<Integer> head2 = iter1.map(noOpIntMap).name("IterForwardMap");
		DataStreamSink<Integer> head3 = iter1.map(noOpIntMap).setParallelism(parallelism / 2).addSink(new ReceiveCheckNoOpSink<Integer>());
		DataStreamSink<Integer> head4 = iter1.map(noOpIntMap).addSink(new ReceiveCheckNoOpSink<Integer>());

		SplitStream<Integer> source3 = env.fromElements(1, 2, 3, 4, 5)
				.map(noOpIntMap).name("EvenOddSourceMap")
				.split(new EvenOddOutputSelector());

		iter1.closeWith(source3.select("even").union(
				head1.rebalance().map(noOpIntMap).broadcast(), head2.shuffle()));

		StreamGraph graph = env.getStreamGraph();

		JobGraph jg = graph.getJobGraph();

		assertEquals(1, graph.getIterationSourceSinkPairs().size());

		Tuple2<StreamNode, StreamNode> sourceSinkPair = graph.getIterationSourceSinkPairs().iterator().next();
		StreamNode itSource = sourceSinkPair.f0;
		StreamNode itSink = sourceSinkPair.f1;

		assertEquals(4, itSource.getOutEdges().size());
		assertEquals(3, itSink.getInEdges().size());

		assertEquals(itSource.getParallelism(), itSink.getParallelism());

		for (StreamEdge edge : itSource.getOutEdges()) {
			if (graph.getTargetVertex(edge).getOperatorName().equals("IterRebalanceMap")) {
				assertTrue(edge.getPartitioner() instanceof RebalancePartitioner);
			} else if (graph.getTargetVertex(edge).getOperatorName().equals("IterForwardMap")) {
				assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
			}
		}
		for (StreamEdge edge : itSink.getInEdges()) {
			if (graph.getStreamNode(edge.getSourceId()).getOperatorName().equals("ParallelizeMapShuffle")) {
				assertTrue(edge.getPartitioner() instanceof ShufflePartitioner);
			}

			if (graph.getStreamNode(edge.getSourceId()).getOperatorName().equals("ParallelizeMapForward")) {
				assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
			}

			if (graph.getStreamNode(edge.getSourceId()).getOperatorName().equals("EvenOddSourceMap")) {
				assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
				assertTrue(edge.getSelectedNames().contains("even"));
			}
		}

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
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source1 = env.fromElements(1, 2, 3, 4, 5)
				.shuffle()
				.map(noOpIntMap);

		DataStream<Integer> source2 = env.fromElements(1, 2, 3, 4, 5)
				.map(noOpIntMap);

		IterativeStream<Integer> iter1 = source1.union(source2).iterate();

		DataStream<Integer> head1 = iter1.map(noOpIntMap).name("map1");
		DataStream<Integer> head2 = iter1.map(noOpIntMap)
				.setParallelism(parallelism / 2)
				.name("shuffle").rebalance();
		DataStreamSink<Integer> head3 = iter1.map(noOpIntMap).setParallelism(parallelism / 2)
				.addSink(new ReceiveCheckNoOpSink<Integer>());
		DataStreamSink<Integer> head4 = iter1.map(noOpIntMap).addSink(new ReceiveCheckNoOpSink<Integer>());

		SplitStream<Integer> source3 = env.fromElements(1, 2, 3, 4, 5)
				.map(noOpIntMap)
				.name("split")
				.split(new EvenOddOutputSelector());

		iter1.closeWith(
				source3.select("even").union(
						head1.map(noOpIntMap).name("bc").broadcast(),
						head2.map(noOpIntMap).shuffle()));

		StreamGraph graph = env.getStreamGraph();

		JobGraph jg = graph.getJobGraph();

		assertEquals(1, graph.getIterationSourceSinkPairs().size());

		Tuple2<StreamNode, StreamNode> sourceSinkPair = graph.getIterationSourceSinkPairs().iterator().next();
		StreamNode itSource = sourceSinkPair.f0;
		StreamNode itSink = sourceSinkPair.f1;

		assertEquals(4, itSource.getOutEdges().size());
		assertEquals(3, itSink.getInEdges().size());

		assertEquals(itSource.getParallelism(), itSink.getParallelism());

		for (StreamEdge edge : itSource.getOutEdges()) {
			if (graph.getTargetVertex(edge).getOperatorName().equals("map1")) {
				assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
				assertEquals(4, graph.getTargetVertex(edge).getParallelism());
			} else if (graph.getTargetVertex(edge).getOperatorName().equals("shuffle")) {
				assertTrue(edge.getPartitioner() instanceof RebalancePartitioner);
				assertEquals(2, graph.getTargetVertex(edge).getParallelism());
			}
		}
		for (StreamEdge edge : itSink.getInEdges()) {
			String tailName = graph.getSourceVertex(edge).getOperatorName();
			if (tailName.equals("split")) {
				assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
				assertTrue(edge.getSelectedNames().contains("even"));
			} else if (tailName.equals("bc")) {
				assertTrue(edge.getPartitioner() instanceof BroadcastPartitioner);
			} else if (tailName.equals("shuffle")) {
				assertTrue(edge.getPartitioner() instanceof ShufflePartitioner);
			}
		}

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
		assertTrue(itSink1.getCoLocationGroup() != null);

		assertEquals(itSource1.getCoLocationGroup(), itSink1.getCoLocationGroup());
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testSimpleIteration() throws Exception {
		int numRetries = 5;
		int timeoutScale = 1;

		for (int numRetry = 0; numRetry < numRetries; numRetry++) {
			try {
				StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				iterated = new boolean[parallelism];

				DataStream<Boolean> source = env.fromCollection(Collections.nCopies(parallelism * 2, false))
						.map(noOpBoolMap).name("ParallelizeMap");

				IterativeStream<Boolean> iteration = source.iterate(3000 * timeoutScale);

				DataStream<Boolean> increment = iteration.flatMap(new IterationHead()).map(noOpBoolMap);

				iteration.map(noOpBoolMap).addSink(new ReceiveCheckNoOpSink());

				iteration.closeWith(increment).addSink(new ReceiveCheckNoOpSink());

				env.execute();

				for (boolean iter : iterated) {
					assertTrue(iter);
				}

				break; // success
			} catch (Throwable t) {
				LOG.info("Run " + (numRetry + 1) + "/" + numRetries + " failed", t);

				if (numRetry >= numRetries - 1) {
					throw t;
				} else {
					timeoutScale *= 2;
				}
			}
		}
	}

	@Test
	public void testCoIteration() throws Exception {
		int numRetries = 5;
		int timeoutScale = 1;

		for (int numRetry = 0; numRetry < numRetries; numRetry++) {
			try {
				TestSink.collected = new ArrayList<>();

				StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				env.setParallelism(2);

				DataStream<String> otherSource = env.fromElements("1000", "2000")
						.map(noOpStrMap).name("ParallelizeMap");

				ConnectedIterativeStreams<Integer, String> coIt = env.fromElements(0, 0)
						.map(noOpIntMap).name("ParallelizeMap")
						.iterate(2000 * timeoutScale)
						.withFeedbackType(Types.STRING);

				try {
					coIt.keyBy(1, 2);
					fail();
				} catch (InvalidProgramException e) {
					// this is expected
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
				}).addSink(new ReceiveCheckNoOpSink<String>());

				coIt.closeWith(head.broadcast().union(otherSource));

				head.addSink(new TestSink()).setParallelism(1);

				assertEquals(1, env.getStreamGraph().getIterationSourceSinkPairs().size());

				env.execute();

				Collections.sort(TestSink.collected);
				assertEquals(Arrays.asList("1", "1", "2", "2", "2", "2"), TestSink.collected);

				break; // success
			} catch (Throwable t) {
				LOG.info("Run " + (numRetry + 1) + "/" + numRetries + " failed", t);

				if (numRetry >= numRetries - 1) {
					throw t;
				} else {
					timeoutScale *= 2;
				}
			}
		}
	}

	/**
	 * This test relies on the hash function used by the {@link DataStream#keyBy}, which is
	 * assumed to be {@link MathUtils#murmurHash}.
	 *
	 * <p>For the test to pass all FlatMappers must see at least two records in the iteration,
	 * which can only be achieved if the hashed values of the input keys map to a complete
	 * congruence system. Given that the test is designed for 3 parallel FlatMapper instances
	 * keys chosen from the [1,3] range are a suitable choice.
     */
	@Test
	public void testGroupByFeedback() throws Exception {
		int numRetries = 5;
		int timeoutScale = 1;

		for (int numRetry = 0; numRetry < numRetries; numRetry++) {
			try {
				StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				env.setParallelism(parallelism - 1);
				env.getConfig().setMaxParallelism(env.getParallelism());

				KeySelector<Integer, Integer> key = new KeySelector<Integer, Integer>() {

					@Override
					public Integer getKey(Integer value) throws Exception {
						return value % 3;
					}
				};

				DataStream<Integer> source = env.fromElements(1, 2, 3)
						.map(noOpIntMap).name("ParallelizeMap");

				IterativeStream<Integer> it = source.keyBy(key).iterate(3000 * timeoutScale);

				DataStream<Integer> head = it.flatMap(new RichFlatMapFunction<Integer, Integer>() {

					int received = 0;
					int key = -1;

					@Override
					public void flatMap(Integer value, Collector<Integer> out) throws Exception {
						received++;
						if (key == -1) {
							key = MathUtils.murmurHash(value % 3) % 3;
						} else {
							assertEquals(key, MathUtils.murmurHash(value % 3) % 3);
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

				it.closeWith(head.keyBy(key).union(head.map(noOpIntMap).keyBy(key))).addSink(new ReceiveCheckNoOpSink<Integer>());

				env.execute();

				break; // success
			} catch (Throwable t) {
				LOG.info("Run " + (numRetry + 1) + "/" + numRetries + " failed", t);

				if (numRetry >= numRetries - 1) {
					throw t;
				} else {
					timeoutScale *= 2;
				}
			}
		}
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testWithCheckPointing() throws Exception {
		int numRetries = 5;
		int timeoutScale = 1;

		for (int numRetry = 0; numRetry < numRetries; numRetry++) {
			try {
				StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

				env.enableCheckpointing();

				DataStream<Boolean> source = env.fromCollection(Collections.nCopies(parallelism * 2, false))
						.map(noOpBoolMap).name("ParallelizeMap");

				IterativeStream<Boolean> iteration = source.iterate(3000 * timeoutScale);

				iteration.closeWith(iteration.flatMap(new IterationHead())).addSink(new ReceiveCheckNoOpSink<Boolean>());

				try {
					env.execute();

					// this statement should never be reached
					fail();
				} catch (UnsupportedOperationException e) {
					// expected behaviour
				}

				// Test force checkpointing

				try {
					env.enableCheckpointing(
						CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME,
						CheckpointingMode.EXACTLY_ONCE,
						false);
					env.execute();

					// this statement should never be reached
					fail();
				} catch (UnsupportedOperationException e) {
					// expected behaviour
				}

				env.enableCheckpointing(
					CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME,
					CheckpointingMode.EXACTLY_ONCE,
					true);
				env.getStreamGraph().getJobGraph();

				break; // success
			} catch (Throwable t) {
				LOG.info("Run " + (numRetry + 1) + "/" + numRetries + " failed", t);

				if (numRetry >= numRetries - 1) {
					throw t;
				} else {
					timeoutScale *= 2;
				}
			}
		}
	}

	private static final class IterationHead extends RichFlatMapFunction<Boolean, Boolean> {
		public void flatMap(Boolean value, Collector<Boolean> out) throws Exception {
			int indx = getRuntimeContext().getIndexOfThisSubtask();
			if (value) {
				iterated[indx] = true;
			} else {
				out.collect(true);
			}
		}
	}

	public static CoMapFunction<Integer, String, String> noOpCoMap = new CoMapFunction<Integer, String, String>() {

		public String map1(Integer value) throws Exception {
			return value.toString();
		}

		public String map2(String value) throws Exception {
			return value;
		}
	};

	public static MapFunction<Integer, Integer> noOpIntMap = new NoOpIntMap();

	public static MapFunction<String, String> noOpStrMap = new MapFunction<String, String>() {

		public String map(String value) throws Exception {
			return value;
		}

	};

	public static CoMapFunction<Integer, Integer, Integer> noOpIntCoMap = new CoMapFunction<Integer, Integer, Integer>() {

		public Integer map1(Integer value) throws Exception {
			return value;
		}

		public Integer map2(Integer value) throws Exception {
			return value;
		}

	};

	public static MapFunction<Boolean, Boolean> noOpBoolMap = new MapFunction<Boolean, Boolean>() {

		public Boolean map(Boolean value) throws Exception {
			return value;
		}

	};

	private static class TestSink implements SinkFunction<String> {

		private static final long serialVersionUID = 1L;
		public static List<String> collected = new ArrayList<String>();

		@Override
		public void invoke(String value) throws Exception {
			collected.add(value);
		}

	}

}
