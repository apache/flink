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

package org.apache.flink.streaming.api.datastream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.temporal.StreamCrossOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.iteration.EndOfIterationPredicate;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamProject;
import org.apache.flink.streaming.api.windowing.deltafunction.DeltaFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.api.windowing.helper.Delta;
import org.apache.flink.streaming.api.windowing.helper.FullStream;
import org.apache.flink.streaming.api.windowing.helper.Timestamp;
import org.apache.flink.streaming.api.windowing.helper.WindowingHelper;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.FieldsPartitioner;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.util.CustomPOJO;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class DataStreamTest {

	private static final long MEMORYSIZE = 32;
	private static int PARALLELISM = 3;

	/**
	 * Tests {@link SingleOutputStreamOperator#name(String)} functionality.
	 *
	 * @throws Exception
	 */
	@Test
	public void testNaming() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);

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

		DataStream<Long> connected = dataStream1.connect(dataStream2)
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
				.flatten();

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
	 * different and correct topologies. Does the some for the {@link ConnectedDataStream}.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testPartitioning() {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);
		StreamGraph graph = env.getStreamGraph();

		DataStream src1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
		DataStream src2 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
		ConnectedDataStream connected = src1.connect(src2);

		//Testing DataStream grouping
		DataStream group1 = src1.groupBy(0);
		DataStream group2 = src1.groupBy(1, 0);
		DataStream group3 = src1.groupBy("f0");
		DataStream group4 = src1.groupBy(new FirstSelector());

		assertTrue(isPartitioned(graph.getStreamEdge(group1.getId(), createDownStreamId(group1))));
		assertTrue(isPartitioned(graph.getStreamEdge(group2.getId(), createDownStreamId(group2))));
		assertTrue(isPartitioned(graph.getStreamEdge(group3.getId(), createDownStreamId(group3))));
		assertTrue(isPartitioned(graph.getStreamEdge(group4.getId(), createDownStreamId(group4))));

		assertTrue(isGrouped(group1));
		assertTrue(isGrouped(group2));
		assertTrue(isGrouped(group3));
		assertTrue(isGrouped(group4));

		//Testing DataStream partitioning
		DataStream partition1 = src1.partitionByHash(0);
		DataStream partition2 = src1.partitionByHash(1, 0);
		DataStream partition3 = src1.partitionByHash("f0");
		DataStream partition4 = src1.partitionByHash(new FirstSelector());

		assertTrue(isPartitioned(graph.getStreamEdge(partition1.getId(), createDownStreamId(partition1))));
		assertTrue(isPartitioned(graph.getStreamEdge(partition2.getId(), createDownStreamId(partition2))));
		assertTrue(isPartitioned(graph.getStreamEdge(partition3.getId(), createDownStreamId(partition3))));
		assertTrue(isPartitioned(graph.getStreamEdge(partition4.getId(), createDownStreamId(partition4))));

		assertFalse(isGrouped(partition1));
		assertFalse(isGrouped(partition3));
		assertFalse(isGrouped(partition2));
		assertFalse(isGrouped(partition4));

		//Testing ConnectedDataStream grouping
		ConnectedDataStream connectedGroup1 = connected.groupBy(0, 0);
		Integer downStreamId1 = createDownStreamId(connectedGroup1);

		ConnectedDataStream connectedGroup2 = connected.groupBy(new int[]{0}, new int[]{0});
		Integer downStreamId2 = createDownStreamId(connectedGroup2);

		ConnectedDataStream connectedGroup3 = connected.groupBy("f0", "f0");
		Integer downStreamId3 = createDownStreamId(connectedGroup3);

		ConnectedDataStream connectedGroup4 = connected.groupBy(new String[]{"f0"}, new String[]{"f0"});
		Integer downStreamId4 = createDownStreamId(connectedGroup4);

		ConnectedDataStream connectedGroup5 = connected.groupBy(new FirstSelector(), new FirstSelector());
		Integer downStreamId5 = createDownStreamId(connectedGroup5);

		assertTrue(isPartitioned(graph.getStreamEdge(connectedGroup1.getFirst().getId(), downStreamId1)));
		assertTrue(isPartitioned(graph.getStreamEdge(connectedGroup1.getSecond().getId(), downStreamId1)));

		assertTrue(isPartitioned(graph.getStreamEdge(connectedGroup2.getFirst().getId(), downStreamId2)));
		assertTrue(isPartitioned(graph.getStreamEdge(connectedGroup2.getSecond().getId(), downStreamId2)));

		assertTrue(isPartitioned(graph.getStreamEdge(connectedGroup3.getFirst().getId(), downStreamId3)));
		assertTrue(isPartitioned(graph.getStreamEdge(connectedGroup3.getSecond().getId(), downStreamId3)));

		assertTrue(isPartitioned(graph.getStreamEdge(connectedGroup4.getFirst().getId(), downStreamId4)));
		assertTrue(isPartitioned(graph.getStreamEdge(connectedGroup4.getSecond().getId(), downStreamId4)));

		assertTrue(isPartitioned(graph.getStreamEdge(connectedGroup5.getFirst().getId(), downStreamId5)));
		assertTrue(isPartitioned(graph.getStreamEdge(connectedGroup5.getSecond().getId(), downStreamId5)));

		assertTrue(isGrouped(connectedGroup1));
		assertTrue(isGrouped(connectedGroup2));
		assertTrue(isGrouped(connectedGroup3));
		assertTrue(isGrouped(connectedGroup4));
		assertTrue(isGrouped(connectedGroup5));

		//Testing ConnectedDataStream partitioning
		ConnectedDataStream connectedPartition1 = connected.partitionByHash(0, 0);
		Integer connectDownStreamId1 = createDownStreamId(connectedPartition1);

		ConnectedDataStream connectedPartition2 = connected.partitionByHash(new int[]{0}, new int[]{0});
		Integer connectDownStreamId2 = createDownStreamId(connectedPartition2);

		ConnectedDataStream connectedPartition3 = connected.partitionByHash("f0", "f0");
		Integer connectDownStreamId3 = createDownStreamId(connectedPartition3);

		ConnectedDataStream connectedPartition4 = connected.partitionByHash(new String[]{"f0"}, new String[]{"f0"});
		Integer connectDownStreamId4 = createDownStreamId(connectedPartition4);

		ConnectedDataStream connectedPartition5 = connected.partitionByHash(new FirstSelector(), new FirstSelector());
		Integer connectDownStreamId5 = createDownStreamId(connectedPartition5);

		assertTrue(isPartitioned(graph.getStreamEdge(connectedPartition1.getFirst().getId(), connectDownStreamId1)));
		assertTrue(isPartitioned(graph.getStreamEdge(connectedPartition1.getSecond().getId(), connectDownStreamId1)));

		assertTrue(isPartitioned(graph.getStreamEdge(connectedPartition2.getFirst().getId(), connectDownStreamId2)));
		assertTrue(isPartitioned(graph.getStreamEdge(connectedPartition2.getSecond().getId(), connectDownStreamId2)));

		assertTrue(isPartitioned(graph.getStreamEdge(connectedPartition3.getFirst().getId(), connectDownStreamId3)));
		assertTrue(isPartitioned(graph.getStreamEdge(connectedPartition3.getSecond().getId(), connectDownStreamId3)));

		assertTrue(isPartitioned(graph.getStreamEdge(connectedPartition4.getFirst().getId(), connectDownStreamId4)));
		assertTrue(isPartitioned(graph.getStreamEdge(connectedPartition4.getSecond().getId(), connectDownStreamId4)));

		assertTrue(isPartitioned(graph.getStreamEdge(connectedPartition5.getFirst().getId(), connectDownStreamId5)));
		assertTrue(isPartitioned(graph.getStreamEdge(connectedPartition5.getSecond().getId(), connectDownStreamId5)));

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
		StreamExecutionEnvironment env = new TestStreamEnvironment(10, MEMORYSIZE);
		StreamGraph graph = env.getStreamGraph();

		DataStreamSource<Tuple2<Long, Long>> src = env.fromElements(new Tuple2<Long, Long>(0L, 0L));

		SingleOutputStreamOperator<Long, ?> map = src.map(new MapFunction<Tuple2<Long, Long>, Long>() {
			@Override
			public Long map(Tuple2<Long, Long> value) throws Exception {
				return null;
			}
		});

		DataStream<Long> windowed = map
				.window(Count.of(10))
				.foldWindow(0L, new FoldFunction<Long, Long>() {
					@Override
					public Long fold(Long accumulator, Long value) throws Exception {
						return null;
					}
				}).flatten();

		DataStreamSink<Long> sink = map.addSink(new SinkFunction<Long>() {
			@Override
			public void invoke(Long value) throws Exception {
			}
		});

		assertEquals(1, graph.getStreamNode(src.getId()).getParallelism());
		assertEquals(10, graph.getStreamNode(map.getId()).getParallelism());
		assertEquals(10, graph.getStreamNode(windowed.getId()).getParallelism());
		assertEquals(10, graph.getStreamNode(sink.getId()).getParallelism());

		env.setParallelism(7);
		assertEquals(1, graph.getStreamNode(src.getId()).getParallelism());
		assertEquals(7, graph.getStreamNode(map.getId()).getParallelism());
		assertEquals(7, graph.getStreamNode(windowed.getId()).getParallelism());
		assertEquals(7, graph.getStreamNode(sink.getId()).getParallelism());

		try {
			src.setParallelism(3);
			fail();
		} catch (IllegalArgumentException success) {
		}

		DataStreamSource<Long> parallelSource = env.generateSequence(0, 0);
		assertEquals(7, graph.getStreamNode(parallelSource.getId()).getParallelism());

		parallelSource.setParallelism(3);
		assertEquals(3, graph.getStreamNode(parallelSource.getId()).getParallelism());

		map.setParallelism(2);
		assertEquals(2, graph.getStreamNode(map.getId()).getParallelism());

		sink.setParallelism(4);
		assertEquals(4, graph.getStreamNode(sink.getId()).getParallelism());
	}

	@Test
	public void testTypeInfo() {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);

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
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);

		StreamGraph streamGraph = env.getStreamGraph();

		DataStreamSource<Long> src = env.generateSequence(0, 0);

		// map
		MapFunction<Long, Integer> mapFunction = new MapFunction<Long, Integer>() {
			@Override
			public Integer map(Long value) throws Exception {
				return null;
			}
		};
		DataStream<Integer> map = src.map(mapFunction);
		assertEquals(mapFunction, getFunctionForDataStream(map));

		// flatmap
		FlatMapFunction<Long, Integer> flatMapFunction = new FlatMapFunction<Long, Integer>() {
			@Override
			public void flatMap(Long value, Collector<Integer> out) throws Exception {
			}
		};
		DataStream<Integer> flatMap = src.flatMap(flatMapFunction);
		assertEquals(flatMapFunction, getFunctionForDataStream(flatMap));

		// filter
		FilterFunction<Integer> filterFunction = new FilterFunction<Integer>() {
			@Override
			public boolean filter(Integer value) throws Exception {
				return false;
			}
		};

		// union
		DataStream<Integer> unionFilter = map
				.union(flatMap)
				.filter(filterFunction);

		assertEquals(filterFunction, getFunctionForDataStream(unionFilter));

		try {
			streamGraph.getStreamEdge(map.getId(), unionFilter.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}

		try {
			streamGraph.getStreamEdge(flatMap.getId(), unionFilter.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}

		// splitting
		OutputSelector<Integer> outputSelector = new OutputSelector<Integer>() {
			@Override
			public Iterable<String> select(Integer value) {
				return null;
			}
		};

		SplitDataStream<Integer> split = unionFilter.split(outputSelector);
		List<OutputSelector<?>> outputSelectors = streamGraph.getStreamNode(split.getId()).getOutputSelectors();
		assertEquals(1, outputSelectors.size());
		assertEquals(outputSelector, outputSelectors.get(0));

		DataStream<Integer> select = split.select("a");
		DataStreamSink<Integer> sink = select.print();

		StreamEdge splitEdge = streamGraph.getStreamEdge(select.getId(), sink.getId());
		assertEquals("a", splitEdge.getSelectedNames().get(0));

		// coMap
		ConnectedDataStream<Integer, Integer> connect = map.connect(flatMap);
		CoMapFunction<Integer, Integer, Integer> coMapper = new CoMapFunction<Integer, Integer, Integer>() {
			@Override
			public Integer map1(Integer value) {
				return null;
			}

			@Override
			public Integer map2(Integer value) {
				return null;
			}
		};
		DataStream<Integer> coMap = connect.map(coMapper);
		assertEquals(coMapper, getFunctionForDataStream(coMap));

		try {
			streamGraph.getStreamEdge(map.getId(), coMap.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}

		try {
			streamGraph.getStreamEdge(flatMap.getId(), coMap.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}

		DataStream<Tuple3<Integer, Integer, Integer>> tuples = map.map(new MapFunction<Integer, Tuple3<Integer, Integer, Integer>>() {
			@Override
			public Tuple3<Integer, Integer, Integer> map(Integer value) throws Exception {
				return null;
			}
		});

		// project
		int[] projectedFields = {0, 2};
		DataStream<Tuple> project = tuples.project(projectedFields);
		StreamProject projectOperator = (StreamProject) getOperatorForDataStream(project);
		assertArrayEquals(projectedFields, projectOperator.getFields());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAggregations() {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);

		StreamGraph streamGraph = env.getStreamGraph();

		GroupedDataStream<CustomPOJO> src = env.fromElements(new CustomPOJO()).groupBy(new KeySelector<CustomPOJO, Integer>() {
			@Override
			public Integer getKey(CustomPOJO value) throws Exception {
				return null;
			}
		});
		GroupedDataStream<Tuple2<Integer, Integer>> tuples = env
				.fromElements(new Tuple2<Integer, Integer>(0, 2), new Tuple2<Integer, Integer>(0, 3))
				.groupBy(0);

		// sum
		DataStream<Tuple2<Integer, Integer>> sum = tuples.sum(1);
		AggregationFunction<Tuple2<Integer, Integer>> sumAggregationFunction =
				(AggregationFunction<Tuple2<Integer, Integer>>) getFunctionForDataStream(sum);
		assertEquals(1, sumAggregationFunction.position);

		// min
		DataStream<Tuple2<Integer, Integer>> min = tuples.min(1);
		AggregationFunction<Tuple2<Integer, Integer>> minAggregationFunction =
				(AggregationFunction<Tuple2<Integer, Integer>>) getFunctionForDataStream(min);
		assertEquals(1, minAggregationFunction.position);

		// max
		DataStream<Tuple2<Integer, Integer>> max = tuples.max(1);
		AggregationFunction<Tuple2<Integer, Integer>> maxAggregationFunction =
				(AggregationFunction<Tuple2<Integer, Integer>>) getFunctionForDataStream(max);
		assertEquals(1, maxAggregationFunction.position);

		DataStream<CustomPOJO> fieldSum = src.sum("i");
		AggregationFunction<CustomPOJO> sumFieldFunction =
				(AggregationFunction<CustomPOJO>) getFunctionForDataStream(fieldSum);
		assertEquals(0, sumFieldFunction.position);

		// maxBy
		DataStream<Tuple2<Integer, Integer>> maxBy = tuples.maxBy(1);
		AggregationFunction<Tuple2<Integer, Integer>> maxByAggregationFunction =
				(AggregationFunction<Tuple2<Integer, Integer>>) getFunctionForDataStream(maxBy);
		assertEquals(1, maxByAggregationFunction.position);

		// minBy
		DataStream<Tuple2<Integer, Integer>> minBy = tuples.minBy(1);
		AggregationFunction<Tuple2<Integer, Integer>> minByAggregationFunction =
				(AggregationFunction<Tuple2<Integer, Integer>>) getFunctionForDataStream(minBy);
		assertEquals(1, minByAggregationFunction.position);

		// maxBy last
		DataStream<Tuple2<Integer, Integer>> lastMaxBy = tuples.maxBy(1, false);
		AggregationFunction<Tuple2<Integer, Integer>> lastMaxByAggregationFunction =
				(AggregationFunction<Tuple2<Integer, Integer>>) getFunctionForDataStream(lastMaxBy);
		assertEquals(1, lastMaxByAggregationFunction.position);

		// minBy last
		DataStream<Tuple2<Integer, Integer>> lastMinBy = tuples.minBy(1, false);
		AggregationFunction<Tuple2<Integer, Integer>> lastMinByAggregationFunction =
				(AggregationFunction<Tuple2<Integer, Integer>>) getFunctionForDataStream(lastMinBy);
		assertEquals(1, lastMinByAggregationFunction.position);

		Tuple2<Integer, Integer> tupleVal3 = new Tuple2<Integer, Integer>(-1, 3);
		Tuple2<Integer, Integer> tupleVal2 = new Tuple2<Integer, Integer>(1, 2);
		Tuple2<Integer, Integer> tupleVal3Last = new Tuple2<Integer, Integer>(30, 3);

		CustomPOJO pojo3 = new CustomPOJO();
		pojo3.setI(3);
		CustomPOJO pojo2 = new CustomPOJO();
		pojo2.setI(2);

		try {
			assertEquals(new Integer(5), sumAggregationFunction.reduce(tupleVal2.copy(), tupleVal3.copy()).f1);
			assertEquals(new Integer(2), minAggregationFunction.reduce(tupleVal2.copy(), tupleVal3.copy()).f1);
			assertEquals(new Integer(3), maxAggregationFunction.reduce(tupleVal2.copy(), tupleVal3.copy()).f1);
			assertEquals(5, sumFieldFunction.reduce(pojo2, pojo3).getI());

			assertEquals(tupleVal2.copy(), minByAggregationFunction.reduce(tupleVal2.copy(), tupleVal3.copy()));
			assertEquals(tupleVal3.copy(), minByAggregationFunction.reduce(tupleVal3.copy(), tupleVal3Last.copy()));

			assertEquals(tupleVal3.copy(), maxByAggregationFunction.reduce(tupleVal2.copy(), tupleVal3.copy()));
			assertEquals(tupleVal3.copy(), maxByAggregationFunction.reduce(tupleVal3.copy(), tupleVal3Last.copy()));

			assertEquals(tupleVal2.copy(), lastMinByAggregationFunction.reduce(tupleVal2.copy(), tupleVal3.copy()));
			assertEquals(tupleVal3Last.copy(), lastMinByAggregationFunction.reduce(tupleVal3.copy(), tupleVal3Last.copy()));

			assertEquals(tupleVal3.copy(), lastMaxByAggregationFunction.reduce(tupleVal2.copy(), tupleVal3.copy()));
			assertEquals(tupleVal3Last.copy(), lastMaxByAggregationFunction.reduce(tupleVal3.copy(), tupleVal3Last.copy()));
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void windowTest() {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);

		StreamGraph streamGraph = env.getStreamGraph();

		DataStreamSource<CustomPOJO> src = env.fromElements(new CustomPOJO());
		DataStreamSource<String> stringSrc = env.fromElements("a");

		Count count = Count.of(5);

		DeltaFunction<Long> deltaFunction = new DeltaFunction<Long>() {
			@Override
			public double getDelta(Long oldDataPoint, Long newDataPoint) {
				return 0;
			}
		};
		Delta<Long> delta = Delta.of(0.1, deltaFunction, 0L);

		Timestamp<CustomPOJO> timestamp1 = new Timestamp<CustomPOJO>() {
			@Override
			public long getTimestamp(CustomPOJO value) {
				return 0;
			}
		};
		Timestamp<String> timestamp2 = new Timestamp<String>() {
			@Override
			public long getTimestamp(String value) {
				return 0;
			}
		};

		// default policies
		WindowedDataStream<CustomPOJO> window = src.window(count);
		WindowingHelper<CustomPOJO> trigger = window.triggerHelper;
		WindowingHelper<CustomPOJO> eviction = window.evictionHelper;

		assertTrue(trigger instanceof Count);
		assertEquals(null, eviction);

		WindowedDataStream<CustomPOJO> every = window.every(delta);
		WindowingHelper<CustomPOJO> trigger2 = every.triggerHelper;
		WindowingHelper<CustomPOJO> eviction2 = every.evictionHelper;

		assertEquals(trigger, eviction2);
		assertTrue(trigger2 instanceof Delta);

		// custom policies
		WindowingHelper policyHelper = new WindowingHelper() {
			@Override
			public EvictionPolicy toEvict() {
				return null;
			}

			@Override
			public TriggerPolicy toTrigger() {
				return null;
			}
		};

		WindowedDataStream<CustomPOJO> windowWithCustomPolicy = src.window(policyHelper);
		WindowingHelper<CustomPOJO> triggerHelper = windowWithCustomPolicy.triggerHelper;
		WindowingHelper<CustomPOJO> evictionHelper = windowWithCustomPolicy.evictionHelper;

		assertEquals(policyHelper, triggerHelper);
		assertEquals(null, evictionHelper);

		WindowedDataStream<CustomPOJO> everyCustomPolicy = windowWithCustomPolicy.every(policyHelper);
		triggerHelper = everyCustomPolicy.triggerHelper;
		evictionHelper = everyCustomPolicy.evictionHelper;

		assertEquals(policyHelper, triggerHelper);
		assertEquals(policyHelper, evictionHelper);

		// cross
		StreamCrossOperator<CustomPOJO, String> cross = src.cross(stringSrc);
		assertEquals(src.getId(), cross.input1.getId());
		assertEquals(stringSrc.getId(), cross.input2.getId());

		WindowedDataStream<Tuple2<CustomPOJO, String>> windowCross = cross
				.onWindow(12L, TimeUnit.MILLISECONDS)
				.every(policyHelper);

		WindowingHelper<Tuple2<CustomPOJO, String>> crossTrigger = windowCross.triggerHelper;
		WindowingHelper<Tuple2<CustomPOJO, String>> crossEviction = windowCross.evictionHelper;

		assertEquals(policyHelper, crossTrigger);
		assertTrue(crossEviction instanceof FullStream);
	}

	@Test
	public void testChannelSelectors() {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);

		StreamGraph streamGraph = env.getStreamGraph();

		DataStreamSource<Long> src = env.generateSequence(0, 0);

		DataStream<Long> broadcast = src.broadcast();
		DataStreamSink<Long> broadcastSink = broadcast.print();
		StreamPartitioner<?> broadcastPartitioner =
				streamGraph.getStreamEdge(broadcast.getId(), broadcastSink.getId()).getPartitioner();
		assertTrue(broadcastPartitioner instanceof BroadcastPartitioner);

		DataStream<Long> shuffle = src.shuffle();
		DataStreamSink<Long> shuffleSink = shuffle.print();
		StreamPartitioner<?> shufflePartitioner =
				streamGraph.getStreamEdge(shuffle.getId(), shuffleSink.getId()).getPartitioner();
		assertTrue(shufflePartitioner instanceof ShufflePartitioner);

		DataStream<Long> forward = src.forward();
		DataStreamSink<Long> forwardSink = forward.print();
		StreamPartitioner<?> forwardPartitioner =
				streamGraph.getStreamEdge(forward.getId(), forwardSink.getId()).getPartitioner();
		assertTrue(forwardPartitioner instanceof RebalancePartitioner);

		DataStream<Long> rebalance = src.rebalance();
		DataStreamSink<Long> rebalanceSink = rebalance.print();
		StreamPartitioner<?> rebalancePartitioner =
				streamGraph.getStreamEdge(rebalance.getId(), rebalanceSink.getId()).getPartitioner();
		assertTrue(rebalancePartitioner instanceof RebalancePartitioner);

		DataStream<Long> global = src.global();
		DataStreamSink<Long> globalSink = global.print();
		StreamPartitioner<?> globalPartitioner =
				streamGraph.getStreamEdge(global.getId(), globalSink.getId()).getPartitioner();
		assertTrue(globalPartitioner instanceof GlobalPartitioner);
	}

	@Test
	public void iterationTest() {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);

		StreamGraph streamGraph = env.getStreamGraph();

		DataStream<Long> src = env.generateSequence(0, 0);

		EndOfIterationPredicate<Long> endOfIterationPredicate = new EndOfIterationPredicate<Long>() {
			@Override
			public boolean isEndOfIteration(Long nextElement) {
				return false;
			}
		};
		IterativeDataStream<Long> iterate = src.iterate(endOfIterationPredicate);
		DataStream<Long> iterationMap = iterate.map(new MapFunction<Long, Long>() {
			@Override
			public Long map(Long value) throws Exception {
				return null;
			}
		});

		iterate.closeWith(iterationMap);

		assertEquals(1, streamGraph.getStreamLoops().size());
		StreamGraph.StreamLoop streamLoop = streamGraph.getStreamLoops().iterator().next();

		assertEquals(endOfIterationPredicate, streamLoop.getEndOfIterationPredicate());

		StreamNode iterationHead = streamLoop.getSource();
		StreamNode iterationTail = streamLoop.getSink();

		try {
			streamGraph.getStreamEdge(src.getId(), iterationMap.getId());
			streamGraph.getStreamEdge(iterationHead.getId(), iterationMap.getId());
			streamGraph.getStreamEdge(iterationMap.getId(), iterationTail.getId());
		} catch (RuntimeException e) {
			fail(e.getMessage());
		}
	}

	/////////////////////////////////////////////////////////////
	// Utilities
	/////////////////////////////////////////////////////////////

	private static StreamOperator<?> getOperatorForDataStream(DataStream<?> dataStream) {
		StreamExecutionEnvironment env = dataStream.getExecutionEnvironment();
		StreamGraph streamGraph = env.getStreamGraph();
		return streamGraph.getStreamNode(dataStream.getId()).getOperator();
	}

	public static Function getFunctionForDataStream(DataStream<?> dataStream) {
		AbstractUdfStreamOperator<?, ?> operator =
				(AbstractUdfStreamOperator<?, ?>) getOperatorForDataStream(dataStream);
		return operator.getUserFunction();
	}

	private static Integer createDownStreamId(DataStream dataStream) {
		return dataStream.print().getId();
	}

	private static boolean isGrouped(DataStream dataStream) {
		return dataStream instanceof GroupedDataStream;
	}

	private static Integer createDownStreamId(ConnectedDataStream dataStream) {
		return dataStream.map(new CoMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Object>() {
			@Override
			public Object map1(Tuple2<Long, Long> value) {
				return null;
			}

			@Override
			public Object map2(Tuple2<Long, Long> value) {
				return null;
			}
		}).getId();
	}

	private static boolean isGrouped(ConnectedDataStream dataStream) {
		return (dataStream.getFirst() instanceof GroupedDataStream && dataStream.getSecond() instanceof GroupedDataStream);
	}

	private static boolean isPartitioned(StreamEdge edge) {
		return edge.getPartitioner() instanceof FieldsPartitioner;
	}

	private static class FirstSelector implements KeySelector<Tuple2<Long, Long>, Long> {
		@Override
		public Long getKey(Tuple2<Long, Long> value) throws Exception {
			return value.f0;
		}
	}

}
