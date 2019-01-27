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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * This verifies that slot sharing groups are correctly forwarded from user job to JobGraph.
 *
 * <p>These tests also implicitly verify that chaining does not work across
 * resource groups/slot sharing groups.
 */
@SuppressWarnings("serial")
public class SlotAllocationTest extends TestLogger {

	@Test
	public void testTwoPipelines() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		FilterFunction<Long> dummyFilter = new FilterFunction<Long>() {
			@Override
			public boolean filter(Long value) {
				return false;
			}
		};

		env.generateSequence(1, 10)
			.filter(dummyFilter).slotSharingGroup("isolated")
			.filter(dummyFilter).slotSharingGroup("default").disableChaining()
			.filter(dummyFilter).slotSharingGroup("group 1")
			.filter(dummyFilter).startNewChain()
			.print().disableChaining();

		// verify that a second pipeline does not inherit the groups from the first pipeline
		env.generateSequence(1, 10)
				.filter(dummyFilter).slotSharingGroup("isolated-2")
				.filter(dummyFilter).slotSharingGroup("default").disableChaining()
				.filter(dummyFilter).slotSharingGroup("group 2")
				.filter(dummyFilter).startNewChain()
				.print().disableChaining();

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		assertEquals(vertices.get(0).getSlotSharingGroup(), vertices.get(3).getSlotSharingGroup());
		assertNotEquals(vertices.get(0).getSlotSharingGroup(), vertices.get(2).getSlotSharingGroup());
		assertNotEquals(vertices.get(3).getSlotSharingGroup(), vertices.get(4).getSlotSharingGroup());
		assertEquals(vertices.get(4).getSlotSharingGroup(), vertices.get(5).getSlotSharingGroup());
		assertEquals(vertices.get(5).getSlotSharingGroup(), vertices.get(6).getSlotSharingGroup());

		int pipelineStart = 6;
		assertEquals(vertices.get(1).getSlotSharingGroup(), vertices.get(pipelineStart + 2).getSlotSharingGroup());
		assertNotEquals(vertices.get(1).getSlotSharingGroup(), vertices.get(pipelineStart + 1).getSlotSharingGroup());
		assertNotEquals(vertices.get(pipelineStart + 2).getSlotSharingGroup(), vertices.get(pipelineStart + 3).getSlotSharingGroup());
		assertEquals(vertices.get(pipelineStart + 3).getSlotSharingGroup(), vertices.get(pipelineStart + 4).getSlotSharingGroup());
		assertEquals(vertices.get(pipelineStart + 4).getSlotSharingGroup(), vertices.get(pipelineStart + 5).getSlotSharingGroup());

	}

	@Test
	public void testUnion() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		FilterFunction<Long> dummyFilter = new FilterFunction<Long>() {
			@Override
			public boolean filter(Long value) {
				return false;
			}
		};

		DataStream<Long> src1 = env.generateSequence(1, 10).name("0");
		DataStream<Long> src2 = env.generateSequence(1, 10).name("1").slotSharingGroup("src-1");

		// this should not inherit group "src-1"
		src1.union(src2).filter(dummyFilter).name("4");

		DataStream<Long> src3 = env.generateSequence(1, 10).name("2").slotSharingGroup("group-1");
		DataStream<Long> src4 = env.generateSequence(1, 10).name("3").slotSharingGroup("group-1");

		// this should inherit "group-1" now
		src3.union(src4).filter(dummyFilter).name("5");

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		JobVertex[] sortedVertices = jobGraph.getVerticesAsArray();
		Arrays.sort(sortedVertices, new Comparator<JobVertex>() {
			@Override
			public int compare(JobVertex o1, JobVertex o2) {
				if (o1.hasNoConnectedInputs() && !o2.hasNoConnectedInputs()) {
					return -1;
				} else if (!o1.hasNoConnectedInputs() && o2.hasNoConnectedInputs()) {
					return 1;
				}

				return o1.getName().compareTo(o2.getName());
			}
		});

		// first pipeline
		assertEquals(sortedVertices[0].getSlotSharingGroup(), sortedVertices[4].getSlotSharingGroup());
		assertNotEquals(sortedVertices[0].getSlotSharingGroup(), sortedVertices[1].getSlotSharingGroup());
		assertNotEquals(sortedVertices[1].getSlotSharingGroup(), sortedVertices[4].getSlotSharingGroup());

		// second pipeline
		assertEquals(sortedVertices[2].getSlotSharingGroup(), sortedVertices[3].getSlotSharingGroup());
		assertEquals(sortedVertices[2].getSlotSharingGroup(), sortedVertices[5].getSlotSharingGroup());
		assertEquals(sortedVertices[3].getSlotSharingGroup(), sortedVertices[5].getSlotSharingGroup());
	}

	@Test
	public void testInheritOverride() {
		// verify that we can explicitly disable inheritance of the input slot sharing groups

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		FilterFunction<Long> dummyFilter = new FilterFunction<Long>() {
			@Override
			public boolean filter(Long value) {
				return false;
			}
		};

		DataStream<Long> src1 = env.generateSequence(1, 10).slotSharingGroup("group-1");
		DataStream<Long> src2 = env.generateSequence(1, 10).slotSharingGroup("group-1");

		// this should not inherit group but be in "default"
		src1.union(src2).filter(dummyFilter).slotSharingGroup("default");
		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		assertEquals(vertices.get(0).getSlotSharingGroup(), vertices.get(1).getSlotSharingGroup());
		assertNotEquals(vertices.get(0).getSlotSharingGroup(), vertices.get(2).getSlotSharingGroup());
		assertNotEquals(vertices.get(1).getSlotSharingGroup(), vertices.get(2).getSlotSharingGroup());
	}

	@Test
	public void testCoOperation() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		CoMapFunction<Long, Long, Long> dummyCoMap = new CoMapFunction<Long, Long, Long>() {
			@Override
			public Long map1(Long value) throws Exception {
				return null;
			}

			@Override
			public Long map2(Long value) throws Exception {
				return null;
			}
		};

		DataStream<Long> src1 = env.generateSequence(1, 10);
		DataStream<Long> src2 = env.generateSequence(1, 10).slotSharingGroup("src-1");

		// this should not inherit group "src-1"
		src1.connect(src2).map(dummyCoMap);

		DataStream<Long> src3 = env.generateSequence(1, 10).slotSharingGroup("group-1");
		DataStream<Long> src4 = env.generateSequence(1, 10).slotSharingGroup("group-1");

		// this should inherit "group-1" now
		src3.connect(src4).map(dummyCoMap);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		// first pipeline
		assertEquals(vertices.get(0).getSlotSharingGroup(), vertices.get(4).getSlotSharingGroup());
		assertNotEquals(vertices.get(0).getSlotSharingGroup(), vertices.get(1).getSlotSharingGroup());
		assertNotEquals(vertices.get(1).getSlotSharingGroup(), vertices.get(4).getSlotSharingGroup());

		// second pipeline
		assertEquals(vertices.get(2).getSlotSharingGroup(), vertices.get(3).getSlotSharingGroup());
		assertEquals(vertices.get(2).getSlotSharingGroup(), vertices.get(5).getSlotSharingGroup());
		assertEquals(vertices.get(3).getSlotSharingGroup(), vertices.get(5).getSlotSharingGroup());
	}
}
