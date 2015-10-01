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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.GroupedDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.HashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.NoOpSink;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class DataStreamTest extends StreamingMultipleProgramsTestBase {


	/**
	 * Tests {@link SingleOutputStreamOperator#name(String)} functionality.
	 *
	 * @throws Exception
	 */
	@Test
	public void testNaming() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Long> dataStream1 = env.generateSequence(0, 0).name("testSource1")
				.map(new MapFunction<Long, Long>() {
					@Override
					public Long map(Long value) throws Exception {
						return null;
					}
				}).name("testMap");

		DataStream<Long> dataStream2 = env.generateSequence(0, 0).name("testSource2")
				.map(new MapFunction<Long, Long>() {
					@Override
					public Long map(Long value) throws Exception {
						return null;
					}
				}).name("testMap");

		DataStreamSink<Long> connected = dataStream1.connect(dataStream2)
				.flatMap(new CoFlatMapFunction<Long, Long, Long>() {
					@Override
					public void flatMap1(Long value, Collector<Long> out) throws Exception {
					}

					@Override
					public void flatMap2(Long value, Collector<Long> out) throws Exception {
					}
				}).name("testCoFlatMap")
				.window(Count.of(10))
				.foldWindow(0L, new FoldFunction<Long, Long>() {
					@Override
					public Long fold(Long accumulator, Long value) throws Exception {
						return null;
					}
				}).name("testWindowFold")
				.flatten()
				.print();

		//test functionality through the operator names in the execution plan
		String plan = env.getExecutionPlan();

		assertTrue(plan.contains("testSource1"));
		assertTrue(plan.contains("testSource2"));
		assertTrue(plan.contains("testMap"));
		assertTrue(plan.contains("testMap"));
		assertTrue(plan.contains("testCoFlatMap"));
		assertTrue(plan.contains("testWindowFold"));
	}

	/**
	 * Tests that {@link DataStream#groupBy} and {@link DataStream#partitionByHash} result in
	 * different and correct topologies. Does the some for the {@link ConnectedStreams}.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testPartitioning() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream src1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
		DataStream src2 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
		ConnectedStreams connected = src1.connect(src2);

		//Testing DataStream grouping
		DataStream group1 = src1.groupBy(0);
		DataStream group2 = src1.groupBy(1, 0);
		DataStream group3 = src1.groupBy("f0");
		DataStream group4 = src1.groupBy(new FirstSelector());

		int id1 = createDownStreamId(group1);
		int id2 = createDownStreamId(group2);
		int id3 = createDownStreamId(group3);
		int id4 = createDownStreamId(group4);

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), id1)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), id2)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), id3)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), id4)));

		assertTrue(isGrouped(group1));
		assertTrue(isGrouped(group2));
		assertTrue(isGrouped(group3));
		assertTrue(isGrouped(group4));

		//Testing DataStream partitioning
		DataStream partition1 = src1.partitionByHash(0);
		DataStream partition2 = src1.partitionByHash(1, 0);
		DataStream partition3 = src1.partitionByHash("f0");
		DataStream partition4 = src1.partitionByHash(new FirstSelector());

		int pid1 = createDownStreamId(partition1);
		int pid2 = createDownStreamId(partition2);
		int pid3 = createDownStreamId(partition3);
		int pid4 = createDownStreamId(partition4);

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), pid1)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), pid2)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), pid3)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), pid4)));

		assertFalse(isGrouped(partition1));
		assertFalse(isGrouped(partition3));
		assertFalse(isGrouped(partition2));
		assertFalse(isGrouped(partition4));

		// Testing DataStream custom partitioning
		Partitioner<Long> longPartitioner = new Partitioner<Long>() {
			@Override
			public int partition(Long key, int numPartitions) {
				return 100;
			}
		};

		DataStream customPartition1 = src1.partitionCustom(longPartitioner, 0);
		DataStream customPartition3 = src1.partitionCustom(longPartitioner, "f0");
		DataStream customPartition4 = src1.partitionCustom(longPartitioner, new FirstSelector());

		int cid1 = createDownStreamId(customPartition1);
		int cid2 = createDownStreamId(customPartition3);
		int cid3 = createDownStreamId(customPartition4);

		assertTrue(isCustomPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), cid1)));
		assertTrue(isCustomPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), cid2)));
		assertTrue(isCustomPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), cid3)));

		assertFalse(isGrouped(customPartition1));
		assertFalse(isGrouped(customPartition3));
		assertFalse(isGrouped(customPartition4));

		//Testing ConnectedStreams grouping
		ConnectedStreams connectedGroup1 = connected.groupBy(0, 0);
		Integer downStreamId1 = createDownStreamId(connectedGroup1);

		ConnectedStreams connectedGroup2 = connected.groupBy(new int[]{0}, new int[]{0});
		Integer downStreamId2 = createDownStreamId(connectedGroup2);

		ConnectedStreams connectedGroup3 = connected.groupBy("f0", "f0");
		Integer downStreamId3 = createDownStreamId(connectedGroup3);

		ConnectedStreams connectedGroup4 = connected.groupBy(new String[]{"f0"}, new String[]{"f0"});
		Integer downStreamId4 = createDownStreamId(connectedGroup4);

		ConnectedStreams connectedGroup5 = connected.groupBy(new FirstSelector(), new FirstSelector());
		Integer downStreamId5 = createDownStreamId(connectedGroup5);

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), downStreamId1)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src2.getId(), downStreamId1)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), downStreamId2)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src2.getId(), downStreamId2)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), downStreamId3)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src2.getId(), downStreamId3)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), downStreamId4)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src2.getId(), downStreamId4)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(), downStreamId5)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src2.getId(), downStreamId5)));

		assertTrue(isGrouped(connectedGroup1));
		assertTrue(isGrouped(connectedGroup2));
		assertTrue(isGrouped(connectedGroup3));
		assertTrue(isGrouped(connectedGroup4));
		assertTrue(isGrouped(connectedGroup5));

		//Testing ConnectedStreams partitioning
		ConnectedStreams connectedPartition1 = connected.partitionByHash(0, 0);
		Integer connectDownStreamId1 = createDownStreamId(connectedPartition1);

		ConnectedStreams connectedPartition2 = connected.partitionByHash(new int[]{0}, new int[]{0});
		Integer connectDownStreamId2 = createDownStreamId(connectedPartition2);

		ConnectedStreams connectedPartition3 = connected.partitionByHash("f0", "f0");
		Integer connectDownStreamId3 = createDownStreamId(connectedPartition3);

		ConnectedStreams connectedPartition4 = connected.partitionByHash(new String[]{"f0"}, new String[]{"f0"});
		Integer connectDownStreamId4 = createDownStreamId(connectedPartition4);

		ConnectedStreams connectedPartition5 = connected.partitionByHash(new FirstSelector(), new FirstSelector());
		Integer connectDownStreamId5 = createDownStreamId(connectedPartition5);

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(),
				connectDownStreamId1)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src2.getId(),
				connectDownStreamId1)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(),
				connectDownStreamId2)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src2.getId(),
				connectDownStreamId2)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(),
				connectDownStreamId3)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src2.getId(),
				connectDownStreamId3)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(),
				connectDownStreamId4)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src2.getId(),
				connectDownStreamId4)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src1.getId(),
				connectDownStreamId5)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdge(src2.getId(),
				connectDownStreamId5)));

		assertFalse(isGrouped(connectedPartition1));
		assertFalse(isGrouped(connectedPartition2));
		assertFalse(isGrouped(connectedPartition3));
		assertFalse(isGrouped(connectedPartition4));
		assertFalse(isGrouped(connectedPartition5));
	}

	/**
	 * Tests whether parallelism gets set.
	 */
	@Test
	public void testParallelism() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Long, Long>> src = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
		env.setParallelism(10);

		SingleOutputStreamOperator<Long, ?> map = src.map(new MapFunction<Tuple2<Long, Long>, Long>() {
			@Override
			public Long map(Tuple2<Long, Long> value) throws Exception {
				return null;
			}
		}).name("MyMap");

		DataStream<Long> windowed = map
				.window(Count.of(10))
				.foldWindow(0L, new FoldFunction<Long, Long>() {
					@Override
					public Long fold(Long accumulator, Long value) throws Exception {
						return null;
					}
				})
				.flatten();

		windowed.addSink(new NoOpSink<Long>());

		DataStreamSink<Long> sink = map.addSink(new SinkFunction<Long>() {
			@Override
			public void invoke(Long value) throws Exception {
			}
		});

		assertEquals(1, env.getStreamGraph().getStreamNode(src.getId()).getParallelism());
		assertEquals(10, env.getStreamGraph().getStreamNode(map.getId()).getParallelism());
		assertEquals(1, env.getStreamGraph().getStreamNode(windowed.getId()).getParallelism());
		assertEquals(10,
				env.getStreamGraph().getStreamNode(sink.getTransformation().getId()).getParallelism());

		env.setParallelism(7);

		// Some parts, such as windowing rely on the fact that previous operators have a parallelism
		// set when instantiating the Discretizer. This would break if we dynamically changed
		// the parallelism of operations when changing the setting on the Execution Environment.
		assertEquals(1, env.getStreamGraph().getStreamNode(src.getId()).getParallelism());
		assertEquals(10, env.getStreamGraph().getStreamNode(map.getId()).getParallelism());
		assertEquals(1, env.getStreamGraph().getStreamNode(windowed.getId()).getParallelism());
		assertEquals(10, env.getStreamGraph().getStreamNode(sink.getTransformation().getId()).getParallelism());

		try {
			src.setParallelism(3);
			fail();
		} catch (IllegalArgumentException success) {
		}

		DataStreamSource<Long> parallelSource = env.generateSequence(0, 0);
		parallelSource.addSink(new NoOpSink<Long>());
		assertEquals(7, env.getStreamGraph().getStreamNode(parallelSource.getId()).getParallelism());

		parallelSource.setParallelism(3);
		assertEquals(3, env.getStreamGraph().getStreamNode(parallelSource.getId()).getParallelism());

		map.setParallelism(2);
		assertEquals(2, env.getStreamGraph().getStreamNode(map.getId()).getParallelism());

		sink.setParallelism(4);
		assertEquals(4, env.getStreamGraph().getStreamNode(sink.getTransformation().getId()).getParallelism());
	}

	@Test
	public void testTypeInfo() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Long> src1 = env.generateSequence(0, 0);
		assertEquals(TypeExtractor.getForClass(Long.class), src1.getType());

		DataStream<Tuple2<Integer, String>> map = src1.map(new MapFunction<Long, Tuple2<Integer, String>>() {
			@Override
			public Tuple2<Integer, String> map(Long value) throws Exception {
				return null;
			}
		});

		assertEquals(TypeExtractor.getForObject(new Tuple2<Integer, String>(0, "")), map.getType());

		WindowedDataStream<String> window = map
				.window(Count.of(5))
				.mapWindow(new WindowMapFunction<Tuple2<Integer, String>, String>() {
					@Override
					public void mapWindow(Iterable<Tuple2<Integer, String>> values, Collector<String> out) throws Exception {
					}
				});

		assertEquals(TypeExtractor.getForClass(String.class), window.getType());

		DataStream<CustomPOJO> flatten = window
				.foldWindow(new CustomPOJO(), new FoldFunction<String, CustomPOJO>() {
					@Override
					public CustomPOJO fold(CustomPOJO accumulator, String value) throws Exception {
						return null;
					}
				})
				.flatten();

		assertEquals(TypeExtractor.getForClass(CustomPOJO.class), flatten.getType());
	}

	@Test
	public void operatorTest() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Long> src = env.generateSequence(0, 0);

		MapFunction<Long, Integer> mapFunction = new MapFunction<Long, Integer>() {
			@Override
			public Integer map(Long value) throws Exception {
				return null;
			}
		};
		DataStream<Integer> map = src.map(mapFunction);
		map.addSink(new NoOpSink<Integer>());
		assertEquals(mapFunction, getFunctionForDataStream(map));


		FlatMapFunction<Long, Integer> flatMapFunction = new FlatMapFunction<Long, Integer>() {
			@Override
			public void flatMap(Long value, Collector<Integer> out) throws Exception {
			}
		};
		DataStream<Integer> flatMap = src.flatMap(flatMapFunction);
		flatMap.addSink(new NoOpSink<Integer>());
		assertEquals(flatMapFunction, getFunctionForDataStream(flatMap));

		FilterFunction<Integer> filterFunction = new FilterFunction<Integer>() {
			@Override
			public boolean filter(Integer value) throws Exception {
				return false;
			}
		};

		DataStream<Integer> unionFilter = map
				.union(flatMap)
				.filter(filterFunction);

		unionFilter.addSink(new NoOpSink<Integer>());

		assertEquals(filterFunction, getFunctionForDataStream(unionFilter));

		try {
			env.getStreamGraph().getStreamEdge(map.getId(), unionFilter.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}

		try {
			env.getStreamGraph().getStreamEdge(flatMap.getId(), unionFilter.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}

		OutputSelector<Integer> outputSelector = new OutputSelector<Integer>() {
			@Override
			public Iterable<String> select(Integer value) {
				return null;
			}
		};

		SplitDataStream<Integer> split = unionFilter.split(outputSelector);
		split.select("dummy").addSink(new NoOpSink<Integer>());
		List<OutputSelector<?>> outputSelectors = env.getStreamGraph().getStreamNode(unionFilter.getId()).getOutputSelectors();
		assertEquals(1, outputSelectors.size());
		assertEquals(outputSelector, outputSelectors.get(0));

		DataStream<Integer> select = split.select("a");
		DataStreamSink<Integer> sink = select.print();

		StreamEdge splitEdge = env.getStreamGraph().getStreamEdge(unionFilter.getId(), sink.getTransformation().getId());
		assertEquals("a", splitEdge.getSelectedNames().get(0));

		ConnectedStreams<Integer, Integer> connect = map.connect(flatMap);
		CoMapFunction<Integer, Integer, String> coMapper = new CoMapFunction<Integer, Integer, String>() {
			@Override
			public String map1(Integer value) {
				return null;
			}

			@Override
			public String map2(Integer value) {
				return null;
			}
		};
		DataStream<String> coMap = connect.map(coMapper);
		coMap.addSink(new NoOpSink<String>());
		assertEquals(coMapper, getFunctionForDataStream(coMap));

		try {
			env.getStreamGraph().getStreamEdge(map.getId(), coMap.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}

		try {
			env.getStreamGraph().getStreamEdge(flatMap.getId(), coMap.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void sinkKeyTest() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSink<Long> sink = env.generateSequence(1, 100).print();
		assertTrue(env.getStreamGraph().getStreamNode(sink.getTransformation().getId()).getStatePartitioner() == null);
		assertTrue(env.getStreamGraph().getStreamNode(sink.getTransformation().getId()).getInEdges().get(0).getPartitioner() instanceof ForwardPartitioner);

		KeySelector<Long, Long> key1 = new KeySelector<Long, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Long getKey(Long value) throws Exception {
				return (long) 0;
			}
		};

		DataStreamSink<Long> sink2 = env.generateSequence(1, 100).keyBy(key1).print();

		assertTrue(env.getStreamGraph().getStreamNode(sink2.getTransformation().getId()).getStatePartitioner() != null);
		assertEquals(key1, env.getStreamGraph().getStreamNode(sink2.getTransformation().getId()).getStatePartitioner());
		assertTrue(env.getStreamGraph().getStreamNode(sink2.getTransformation().getId()).getInEdges().get(0).getPartitioner() instanceof HashPartitioner);

		KeySelector<Long, Long> key2 = new KeySelector<Long, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Long getKey(Long value) throws Exception {
				return (long) 0;
			}
		};

		DataStreamSink<Long> sink3 = env.generateSequence(1, 100).keyBy(key2).print();

		assertTrue(env.getStreamGraph().getStreamNode(sink3.getTransformation().getId()).getStatePartitioner() != null);
		assertEquals(key2, env.getStreamGraph().getStreamNode(sink3.getTransformation().getId()).getStatePartitioner());
		assertTrue(env.getStreamGraph().getStreamNode(sink3.getTransformation().getId()).getInEdges().get(0).getPartitioner() instanceof HashPartitioner);
	}

	@Test
	public void testChannelSelectors() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Long> src = env.generateSequence(0, 0);

		DataStream<Long> broadcast = src.broadcast();
		DataStreamSink<Long> broadcastSink = broadcast.print();
		StreamPartitioner<?> broadcastPartitioner =
				env.getStreamGraph().getStreamEdge(src.getId(),
						broadcastSink.getTransformation().getId()).getPartitioner();
		assertTrue(broadcastPartitioner instanceof BroadcastPartitioner);

		DataStream<Long> shuffle = src.shuffle();
		DataStreamSink<Long> shuffleSink = shuffle.print();
		StreamPartitioner<?> shufflePartitioner =
				env.getStreamGraph().getStreamEdge(src.getId(),
						shuffleSink.getTransformation().getId()).getPartitioner();
		assertTrue(shufflePartitioner instanceof ShufflePartitioner);

		DataStream<Long> forward = src.forward();
		DataStreamSink<Long> forwardSink = forward.print();
		StreamPartitioner<?> forwardPartitioner =
				env.getStreamGraph().getStreamEdge(src.getId(),
						forwardSink.getTransformation().getId()).getPartitioner();
		assertTrue(forwardPartitioner instanceof ForwardPartitioner);

		DataStream<Long> rebalance = src.rebalance();
		DataStreamSink<Long> rebalanceSink = rebalance.print();
		StreamPartitioner<?> rebalancePartitioner =
				env.getStreamGraph().getStreamEdge(src.getId(),
						rebalanceSink.getTransformation().getId()).getPartitioner();
		assertTrue(rebalancePartitioner instanceof RebalancePartitioner);

		DataStream<Long> global = src.global();
		DataStreamSink<Long> globalSink = global.print();
		StreamPartitioner<?> globalPartitioner =
				env.getStreamGraph().getStreamEdge(src.getId(),
						globalSink.getTransformation().getId()).getPartitioner();
		assertTrue(globalPartitioner instanceof GlobalPartitioner);
	}

	/////////////////////////////////////////////////////////////
	// Utilities
	/////////////////////////////////////////////////////////////

	private static StreamOperator<?> getOperatorForDataStream(DataStream<?> dataStream) {
		StreamExecutionEnvironment env = dataStream.getExecutionEnvironment();
		StreamGraph streamGraph = env.getStreamGraph();
		return streamGraph.getStreamNode(dataStream.getId()).getOperator();
	}

	private static Function getFunctionForDataStream(DataStream<?> dataStream) {
		AbstractUdfStreamOperator<?, ?> operator =
				(AbstractUdfStreamOperator<?, ?>) getOperatorForDataStream(dataStream);
		return operator.getUserFunction();
	}

	private static Integer createDownStreamId(DataStream dataStream) {
		return dataStream.print().getTransformation().getId();
	}

	private static boolean isGrouped(DataStream dataStream) {
		return dataStream instanceof GroupedDataStream;
	}

	private static Integer createDownStreamId(ConnectedStreams dataStream) {
		SingleOutputStreamOperator coMap = dataStream.map(new CoMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Object>() {
			@Override
			public Object map1(Tuple2<Long, Long> value) {
				return null;
			}

			@Override
			public Object map2(Tuple2<Long, Long> value) {
				return null;
			}
		});
		coMap.addSink(new NoOpSink());
		return coMap.getId();
	}

	private static boolean isGrouped(ConnectedStreams dataStream) {
		return (dataStream.getFirstInput() instanceof GroupedDataStream && dataStream.getSecondInput() instanceof GroupedDataStream);
	}

	private static boolean isPartitioned(StreamEdge edge) {
		return edge.getPartitioner() instanceof HashPartitioner;
	}

	private static boolean isCustomPartitioned(StreamEdge edge) {
		return edge.getPartitioner() instanceof CustomPartitionerWrapper;
	}

	private static class FirstSelector implements KeySelector<Tuple2<Long, Long>, Long> {
		@Override
		public Long getKey(Tuple2<Long, Long> value) throws Exception {
			return value.f0;
		}
	}

	public static class CustomPOJO {
		private String s;
		private int i;

		public CustomPOJO() {
		}

		public void setS(String s) {
			this.s = s;
		}

		public void setI(int i) {
			this.i = i;
		}

		public String getS() {
			return s;
		}

		public int getI() {
			return i;
		}
	}
}
