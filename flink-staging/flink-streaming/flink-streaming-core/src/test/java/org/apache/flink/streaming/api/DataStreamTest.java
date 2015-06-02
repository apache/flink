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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.ConnectedDataStream;
import org.apache.flink.streaming.api.datastream.GroupedDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.runtime.partitioner.FieldsPartitioner;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataStreamTest {

	private static final long MEMORYSIZE = 32;
	private static int PARALLELISM = 1;

	/**
	 * Tests {@link SingleOutputStreamOperator#name(String)} functionality.
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
				.reduce(new ReduceFunction<Long>() {
					@Override
					public Long reduce(Long value1, Long value2) throws Exception {
						return null;
					}
				}).name("testReduce");

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
		assertTrue(plan.contains("testReduce"));
		assertTrue(plan.contains("testCoFlatMap"));
		assertTrue(plan.contains("testWindowFold"));
	}

	/**
	 * Tests that {@link DataStream#groupBy} and {@link DataStream#partitionByHash} result in
	 * different and correct topologies. Does the some for the {@link ConnectedDataStream}.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testPartitioning(){
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);
		StreamGraph graph = env.getStreamGraph();

		DataStream src1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
		DataStream src2 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));
		ConnectedDataStream connected = src1.connect(src2);

		//Testing DataStream grouping
		DataStream group1 = src1.groupBy(0);
		DataStream group2 = src1.groupBy(1,0);
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
		ConnectedDataStream connectedGroup1 = connected.groupBy(0,0);
		Integer downStreamId1 = createDownStreamId(connectedGroup1);

		ConnectedDataStream connectedGroup2 = connected.groupBy(new int[]{0},new int[]{0});
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

		ConnectedDataStream connectedPartition2 = connected.partitionByHash(new int[]{0},new int[]{0});
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

	/////////////////////////////////////////////////////////////
	// Utilities
	/////////////////////////////////////////////////////////////

	private static Integer createDownStreamId(DataStream dataStream){
		return dataStream.print().getId();
	}

	private static boolean isGrouped(DataStream dataStream){
		return dataStream instanceof GroupedDataStream;
	}

	private static Integer createDownStreamId(ConnectedDataStream dataStream){
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

	private static boolean isGrouped(ConnectedDataStream dataStream){
		return (dataStream.getFirst() instanceof GroupedDataStream && dataStream.getSecond() instanceof GroupedDataStream);
	}

	private static boolean isPartitioned(StreamEdge edge){
		return edge.getPartitioner() instanceof FieldsPartitioner;
	}

	private static class FirstSelector implements KeySelector<Tuple2<Long, Long>, Long>{
		@Override
		public Long getKey(Tuple2<Long, Long> value) throws Exception {
			return value.f0;
		}
	}
}
