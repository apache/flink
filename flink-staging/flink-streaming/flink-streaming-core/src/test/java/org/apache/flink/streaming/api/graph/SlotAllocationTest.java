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

import static org.junit.Assert.*;

import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Test;

public class SlotAllocationTest extends StreamingMultipleProgramsTestBase{

	@SuppressWarnings("serial")
	@Test
	public void test() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		FilterFunction<Long> dummyFilter = new FilterFunction<Long>() {

			@Override
			public boolean filter(Long value) throws Exception {

				return false;
			}
		};

		env.generateSequence(1, 10).filter(dummyFilter).isolateResources().filter(dummyFilter)
				.disableChaining().filter(dummyFilter).startNewResourceGroup().filter(dummyFilter)
				.startNewChain().print();

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		assertEquals(vertices.get(0).getSlotSharingGroup(), vertices.get(2).getSlotSharingGroup());
		assertNotEquals(vertices.get(0).getSlotSharingGroup(), vertices.get(1)
				.getSlotSharingGroup());
		assertNotEquals(vertices.get(2).getSlotSharingGroup(), vertices.get(3)
				.getSlotSharingGroup());
		assertEquals(vertices.get(3).getSlotSharingGroup(), vertices.get(4).getSlotSharingGroup());

	}
}
