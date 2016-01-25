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
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
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

import static org.junit.Assert.*;

public class DataStreamTest extends StreamingMultipleProgramsTestBase {

	/**
	 * Tests union functionality. This ensures that self-unions and unions of streams
	 * with differing parallelism work.
	 *
	 * @throws Exception
	 */
	@Test
	public void testUnion() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataStream<Long> input1 = env.generateSequence(0, 0)
			.map(new MapFunction<Long, Long>() {
				@Override
				public Long map(Long value) throws Exception {
					return null;
				}
			});

		DataStream<Long> selfUnion = input1.union(input1).map(new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				return null;
			}
		});

		DataStream<Long> input6 = env.generateSequence(0, 0)
			.map(new MapFunction<Long, Long>() {
				@Override
				public Long map(Long value) throws Exception {
					return null;
				}
			});

		DataStream<Long> selfUnionDifferentPartition = input6.broadcast().union(input6).map(new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				return null;
			}
		});

		DataStream<Long> input2 = env.generateSequence(0, 0)
			.map(new MapFunction<Long, Long>() {
				@Override
				public Long map(Long value) throws Exception {
					return null;
				}
			}).setParallelism(4);

		DataStream<Long> input3 = env.generateSequence(0, 0)
			.map(new MapFunction<Long, Long>() {
				@Override
				public Long map(Long value) throws Exception {
					return null;
				}
			}).setParallelism(2);

		DataStream<Long> unionDifferingParallelism= input2.union(input3).map(new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				return null;
			}
		}).setParallelism(4);

		DataStream<Long> input4 = env.generateSequence(0, 0)
			.map(new MapFunction<Long, Long>() {
				@Override
				public Long map(Long value) throws Exception {
					return null;
				}
			}).setParallelism(2);

		DataStream<Long> input5 = env.generateSequence(0, 0)
			.map(new MapFunction<Long, Long>() {
				@Override
				public Long map(Long value) throws Exception {
					return null;
				}
			}).setParallelism(4);

		DataStream<Long> unionDifferingPartitioning = input4.broadcast().union(input5).map(new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				return null;
			}
		}).setParallelism(4);

		StreamGraph streamGraph = env.getStreamGraph();

		// verify self union
		assertTrue(streamGraph.getStreamNode(selfUnion.getId()).getInEdges().size() == 2);
		for (StreamEdge edge: streamGraph.getStreamNode(selfUnion.getId()).getInEdges()) {
			assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
		}

		// verify self union with differnt partitioners
		assertTrue(streamGraph.getStreamNode(selfUnionDifferentPartition.getId()).getInEdges().size() == 2);
		boolean hasForward = false;
		boolean hasBroadcast = false;
		for (StreamEdge edge: streamGraph.getStreamNode(selfUnionDifferentPartition.getId()).getInEdges()) {
			if (edge.getPartitioner() instanceof ForwardPartitioner) {
				hasForward = true;
			}
			if (edge.getPartitioner() instanceof BroadcastPartitioner) {
				hasBroadcast = true;
			}
		}
		assertTrue(hasForward && hasBroadcast);

		// verify union of streams with differing parallelism
		assertTrue(streamGraph.getStreamNode(unionDifferingParallelism.getId()).getInEdges().size() == 2);
		for (StreamEdge edge: streamGraph.getStreamNode(unionDifferingParallelism.getId()).getInEdges()) {
			if (edge.getSourceId() == input2.getId()) {
				assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
			} else if (edge.getSourceId() == input3.getId()) {
				assertTrue(edge.getPartitioner() instanceof RebalancePartitioner);
			} else {
				fail("Wrong input edge.");
			}
		}

		// verify union of streams with differing partitionings
		assertTrue(streamGraph.getStreamNode(unionDifferingPartitioning.getId()).getInEdges().size() == 2);
		for (StreamEdge edge: streamGraph.getStreamNode(unionDifferingPartitioning.getId()).getInEdges()) {
			if (edge.getSourceId() == input4.getId()) {
				assertTrue(edge.getPartitioner() instanceof BroadcastPartitioner);
			} else if (edge.getSourceId() == input5.getId()) {
				assertTrue(edge.getPartitioner() instanceof ForwardPartitioner);
			} else {
				fail("Wrong input edge.");
			}
		}
	}

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
					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap1(Long value, Collector<Long> out) throws Exception {
					}

					@Override
					public void flatMap2(Long value, Collector<Long> out) throws Exception {
					}
				}).name("testCoFlatMap")
				.windowAll(GlobalWindows.create())
				.trigger(PurgingTrigger.of(CountTrigger.of(10)))
				.fold(0L, new FoldFunction<Long, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Long fold(Long accumulator, Long value) throws Exception {
						return null;
					}
				})
				.name("testWindowFold")
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
	 * Tests that {@link DataStream#keyBy} and {@link DataStream#partitionByHash} result in
	 * different and correct topologies. Does the some for the {@link ConnectedStreams}.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testPartitioning() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<Long, Long>> src1 = env.fromElements(new Tuple2<>(0L, 0L));
		DataStream<Tuple2<Long, Long>> src2 = env.fromElements(new Tuple2<>(0L, 0L));
		ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connected = src1.connect(src2);

		//Testing DataStream grouping
		DataStream<Tuple2<Long, Long>> group1 = src1.keyBy(0);
		DataStream<Tuple2<Long, Long>> group2 = src1.keyBy(1, 0);
		DataStream<Tuple2<Long, Long>> group3 = src1.keyBy("f0");
		DataStream<Tuple2<Long, Long>> group4 = src1.keyBy(new FirstSelector());

		int id1 = createDownStreamId(group1);
		int id2 = createDownStreamId(group2);
		int id3 = createDownStreamId(group3);
		int id4 = createDownStreamId(group4);

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), id1)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), id2)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), id3)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), id4)));

		assertTrue(isKeyed(group1));
		assertTrue(isKeyed(group2));
		assertTrue(isKeyed(group3));
		assertTrue(isKeyed(group4));

		//Testing DataStream partitioning
		DataStream<Tuple2<Long, Long>> partition1 = src1.partitionByHash(0);
		DataStream<Tuple2<Long, Long>> partition2 = src1.partitionByHash(1, 0);
		DataStream<Tuple2<Long, Long>> partition3 = src1.partitionByHash("f0");
		DataStream<Tuple2<Long, Long>> partition4 = src1.partitionByHash(new FirstSelector());

		int pid1 = createDownStreamId(partition1);
		int pid2 = createDownStreamId(partition2);
		int pid3 = createDownStreamId(partition3);
		int pid4 = createDownStreamId(partition4);

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), pid1)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), pid2)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), pid3)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), pid4)));

		assertFalse(isKeyed(partition1));
		assertFalse(isKeyed(partition3));
		assertFalse(isKeyed(partition2));
		assertFalse(isKeyed(partition4));

		// Testing DataStream custom partitioning
		Partitioner<Long> longPartitioner = new Partitioner<Long>() {
			@Override
			public int partition(Long key, int numPartitions) {
				return 100;
			}
		};

		DataStream<Tuple2<Long, Long>> customPartition1 = src1.partitionCustom(longPartitioner, 0);
		DataStream<Tuple2<Long, Long>> customPartition3 = src1.partitionCustom(longPartitioner, "f0");
		DataStream<Tuple2<Long, Long>> customPartition4 = src1.partitionCustom(longPartitioner, new FirstSelector());

		int cid1 = createDownStreamId(customPartition1);
		int cid2 = createDownStreamId(customPartition3);
		int cid3 = createDownStreamId(customPartition4);

		assertTrue(isCustomPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), cid1)));
		assertTrue(isCustomPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), cid2)));
		assertTrue(isCustomPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), cid3)));

		assertFalse(isKeyed(customPartition1));
		assertFalse(isKeyed(customPartition3));
		assertFalse(isKeyed(customPartition4));

		//Testing ConnectedStreams grouping
		ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedGroup1 = connected.keyBy(0, 0);
		Integer downStreamId1 = createDownStreamId(connectedGroup1);

		ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedGroup2 = connected.keyBy(new int[]{0}, new int[]{0});
		Integer downStreamId2 = createDownStreamId(connectedGroup2);

		ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedGroup3 = connected.keyBy("f0", "f0");
		Integer downStreamId3 = createDownStreamId(connectedGroup3);

		ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedGroup4 = connected.keyBy(new String[]{"f0"}, new String[]{"f0"});
		Integer downStreamId4 = createDownStreamId(connectedGroup4);

		ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedGroup5 = connected.keyBy(new FirstSelector(), new FirstSelector());
		Integer downStreamId5 = createDownStreamId(connectedGroup5);

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), downStreamId1)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src2.getId(), downStreamId1)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), downStreamId2)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src2.getId(), downStreamId2)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), downStreamId3)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src2.getId(), downStreamId3)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), downStreamId4)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src2.getId(), downStreamId4)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(), downStreamId5)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src2.getId(), downStreamId5)));

		assertTrue(isKeyed(connectedGroup1));
		assertTrue(isKeyed(connectedGroup2));
		assertTrue(isKeyed(connectedGroup3));
		assertTrue(isKeyed(connectedGroup4));
		assertTrue(isKeyed(connectedGroup5));

		//Testing ConnectedStreams partitioning
		ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedPartition1 = connected.partitionByHash(0, 0);
		Integer connectDownStreamId1 = createDownStreamId(connectedPartition1);

		ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedPartition2 = connected.partitionByHash(new int[]{0}, new int[]{0});
		Integer connectDownStreamId2 = createDownStreamId(connectedPartition2);

		ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedPartition3 = connected.partitionByHash("f0", "f0");
		Integer connectDownStreamId3 = createDownStreamId(connectedPartition3);

		ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedPartition4 = connected.partitionByHash(new String[]{"f0"}, new String[]{"f0"});
		Integer connectDownStreamId4 = createDownStreamId(connectedPartition4);

		ConnectedStreams<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedPartition5 = connected.partitionByHash(new FirstSelector(), new FirstSelector());
		Integer connectDownStreamId5 = createDownStreamId(connectedPartition5);

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(),
				connectDownStreamId1)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src2.getId(),
				connectDownStreamId1)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(),
				connectDownStreamId2)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src2.getId(),
				connectDownStreamId2)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(),
				connectDownStreamId3)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src2.getId(),
				connectDownStreamId3)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(),
				connectDownStreamId4)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src2.getId(),
				connectDownStreamId4)));

		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src1.getId(),
				connectDownStreamId5)));
		assertTrue(isPartitioned(env.getStreamGraph().getStreamEdges(src2.getId(),
				connectDownStreamId5)));

		assertFalse(isKeyed(connectedPartition1));
		assertFalse(isKeyed(connectedPartition2));
		assertFalse(isKeyed(connectedPartition3));
		assertFalse(isKeyed(connectedPartition4));
		assertFalse(isKeyed(connectedPartition5));
	}

	/**
	 * Tests whether parallelism gets set.
	 */
	@Test
	public void testParallelism() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Long, Long>> src = env.fromElements(new Tuple2<>(0L, 0L));
		env.setParallelism(10);

		SingleOutputStreamOperator<Long, ?> map = src.map(new MapFunction<Tuple2<Long, Long>, Long>() {
			@Override
			public Long map(Tuple2<Long, Long> value) throws Exception {
				return null;
			}
		}).name("MyMap");

		DataStream<Long> windowed = map
				.windowAll(GlobalWindows.create())
				.trigger(PurgingTrigger.of(CountTrigger.of(10)))
				.fold(0L, new FoldFunction<Long, Long>() {
					@Override
					public Long fold(Long accumulator, Long value) throws Exception {
						return null;
					}
				});

		windowed.addSink(new NoOpSink<Long>());

		DataStreamSink<Long> sink = map.addSink(new SinkFunction<Long>() {
			private static final long serialVersionUID = 1L;

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
			// do nothing
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

		assertEquals(TypeExtractor.getForObject(new Tuple2<>(0, "")), map.getType());

		DataStream<String> window = map
				.windowAll(GlobalWindows.create())
				.trigger(PurgingTrigger.of(CountTrigger.of(5)))
				.apply(new AllWindowFunction<Iterable<Tuple2<Integer, String>>, String, GlobalWindow>() {
					@Override
					public void apply(GlobalWindow window,
							Iterable<Tuple2<Integer, String>> values,
							Collector<String> out) throws Exception {

					}
				});

		assertEquals(TypeExtractor.getForClass(String.class), window.getType());

		DataStream<CustomPOJO> flatten = window
				.windowAll(GlobalWindows.create())
				.trigger(PurgingTrigger.of(CountTrigger.of(5)))
				.fold(new CustomPOJO(), new FoldFunction<String, CustomPOJO>() {
					private static final long serialVersionUID = 1L;

					@Override
					public CustomPOJO fold(CustomPOJO accumulator, String value) throws Exception {
						return null;
					}
				});

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
			private static final long serialVersionUID = 1L;

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

		DataStream<Integer> unionFilter = map.union(flatMap)
				.filter(filterFunction);

		unionFilter.addSink(new NoOpSink<Integer>());

		assertEquals(filterFunction, getFunctionForDataStream(unionFilter));

		try {
			env.getStreamGraph().getStreamEdges(map.getId(), unionFilter.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}

		try {
			env.getStreamGraph().getStreamEdges(flatMap.getId(), unionFilter.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}

		OutputSelector<Integer> outputSelector = new OutputSelector<Integer>() {
			@Override
			public Iterable<String> select(Integer value) {
				return null;
			}
		};

		SplitStream<Integer> split = unionFilter.split(outputSelector);
		split.select("dummy").addSink(new NoOpSink<Integer>());
		List<OutputSelector<?>> outputSelectors = env.getStreamGraph().getStreamNode(unionFilter.getId()).getOutputSelectors();
		assertEquals(1, outputSelectors.size());
		assertEquals(outputSelector, outputSelectors.get(0));

		DataStream<Integer> select = split.select("a");
		DataStreamSink<Integer> sink = select.print();

		StreamEdge splitEdge = env.getStreamGraph().getStreamEdges(unionFilter.getId(), sink.getTransformation().getId()).get(0);
		assertEquals("a", splitEdge.getSelectedNames().get(0));

		ConnectedStreams<Integer, Integer> connect = map.connect(flatMap);
		CoMapFunction<Integer, Integer, String> coMapper = new CoMapFunction<Integer, Integer, String>() {
			private static final long serialVersionUID = 1L;

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
			env.getStreamGraph().getStreamEdges(map.getId(), coMap.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}

		try {
			env.getStreamGraph().getStreamEdges(flatMap.getId(), coMap.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void sinkKeyTest() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSink<Long> sink = env.generateSequence(1, 100).print();
		assertTrue(env.getStreamGraph().getStreamNode(sink.getTransformation().getId()).getStatePartitioner1() == null);
		assertTrue(env.getStreamGraph().getStreamNode(sink.getTransformation().getId()).getInEdges().get(0).getPartitioner() instanceof ForwardPartitioner);

		KeySelector<Long, Long> key1 = new KeySelector<Long, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Long getKey(Long value) throws Exception {
				return (long) 0;
			}
		};

		DataStreamSink<Long> sink2 = env.generateSequence(1, 100).keyBy(key1).print();

		assertNotNull(env.getStreamGraph().getStreamNode(sink2.getTransformation().getId()).getStatePartitioner1());
		assertNotNull(env.getStreamGraph().getStreamNode(sink2.getTransformation().getId()).getStateKeySerializer());
		assertNotNull(env.getStreamGraph().getStreamNode(sink2.getTransformation().getId()).getStateKeySerializer());
		assertEquals(key1, env.getStreamGraph().getStreamNode(sink2.getTransformation().getId()).getStatePartitioner1());
		assertTrue(env.getStreamGraph().getStreamNode(sink2.getTransformation().getId()).getInEdges().get(0).getPartitioner() instanceof HashPartitioner);

		KeySelector<Long, Long> key2 = new KeySelector<Long, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Long getKey(Long value) throws Exception {
				return (long) 0;
			}
		};

		DataStreamSink<Long> sink3 = env.generateSequence(1, 100).keyBy(key2).print();

		assertTrue(env.getStreamGraph().getStreamNode(sink3.getTransformation().getId()).getStatePartitioner1() != null);
		assertEquals(key2, env.getStreamGraph().getStreamNode(sink3.getTransformation().getId()).getStatePartitioner1());
		assertTrue(env.getStreamGraph().getStreamNode(sink3.getTransformation().getId()).getInEdges().get(0).getPartitioner() instanceof HashPartitioner);
	}

	@Test
	public void testChannelSelectors() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Long> src = env.generateSequence(0, 0);

		DataStream<Long> broadcast = src.broadcast();
		DataStreamSink<Long> broadcastSink = broadcast.print();
		StreamPartitioner<?> broadcastPartitioner =
				env.getStreamGraph().getStreamEdges(src.getId(),
						broadcastSink.getTransformation().getId()).get(0).getPartitioner();
		assertTrue(broadcastPartitioner instanceof BroadcastPartitioner);

		DataStream<Long> shuffle = src.shuffle();
		DataStreamSink<Long> shuffleSink = shuffle.print();
		StreamPartitioner<?> shufflePartitioner =
				env.getStreamGraph().getStreamEdges(src.getId(),
						shuffleSink.getTransformation().getId()).get(0).getPartitioner();
		assertTrue(shufflePartitioner instanceof ShufflePartitioner);

		DataStream<Long> forward = src.forward();
		DataStreamSink<Long> forwardSink = forward.print();
		StreamPartitioner<?> forwardPartitioner =
				env.getStreamGraph().getStreamEdges(src.getId(),
						forwardSink.getTransformation().getId()).get(0).getPartitioner();
		assertTrue(forwardPartitioner instanceof ForwardPartitioner);

		DataStream<Long> rebalance = src.rebalance();
		DataStreamSink<Long> rebalanceSink = rebalance.print();
		StreamPartitioner<?> rebalancePartitioner =
				env.getStreamGraph().getStreamEdges(src.getId(),
						rebalanceSink.getTransformation().getId()).get(0).getPartitioner();
		assertTrue(rebalancePartitioner instanceof RebalancePartitioner);

		DataStream<Long> global = src.global();
		DataStreamSink<Long> globalSink = global.print();
		StreamPartitioner<?> globalPartitioner =
				env.getStreamGraph().getStreamEdges(src.getId(),
						globalSink.getTransformation().getId()).get(0).getPartitioner();
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

	private static Integer createDownStreamId(DataStream<?> dataStream) {
		return dataStream.print().getTransformation().getId();
	}

	private static boolean isKeyed(DataStream<?> dataStream) {
		return dataStream instanceof KeyedStream;
	}

	@SuppressWarnings("rawtypes,unchecked")
	private static Integer createDownStreamId(ConnectedStreams dataStream) {
		SingleOutputStreamOperator<?, ?> coMap = dataStream.map(new CoMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Object>() {
			private static final long serialVersionUID = 1L;

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

	private static boolean isKeyed(ConnectedStreams<?, ?> dataStream) {
		return (dataStream.getFirstInput() instanceof KeyedStream && dataStream.getSecondInput() instanceof KeyedStream);
	}

	private static boolean isPartitioned(List<StreamEdge> edges) {
		boolean result = true;
		for (StreamEdge edge: edges) {
			if (!(edge.getPartitioner() instanceof HashPartitioner)) {
				result = false;
			}
		}
		return result;
	}

	private static boolean isCustomPartitioned(List<StreamEdge> edges) {
		boolean result = true;
		for (StreamEdge edge: edges) {
			if (!(edge.getPartitioner() instanceof CustomPartitionerWrapper)) {
				result = false;
			}
		}
		return result;
	}

	private static class FirstSelector implements KeySelector<Tuple2<Long, Long>, Long> {
		private static final long serialVersionUID = 1L;

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
