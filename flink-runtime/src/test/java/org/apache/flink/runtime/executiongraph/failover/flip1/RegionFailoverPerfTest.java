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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Perf test for region building.
 */
public class RegionFailoverPerfTest extends TestLogger {

	@Test
	public void complexPerfTest() {
		final int parallelism = 2000;

		final FailoverTopology topology = buildComplexTopology(parallelism);

		final long start = System.nanoTime();
		final RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);
		final long end = System.nanoTime();
		System.out.println("build regions: " + (end - start) / 1000000);
	}

	public FailoverTopology buildComplexTopology(int parallelism) {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		int pa = parallelism;
		HashSet<TestFailoverTopology.TestFailoverVertex> aSet = new HashSet<>();
		for (int i = 0; i < pa; i++) {
			TestFailoverTopology.TestFailoverVertex v = topologyBuilder.newVertex();
			aSet.add(v);
		}

		int pb = pa;
		HashSet<TestFailoverTopology.TestFailoverVertex> bSet = new HashSet<>();
		for (int i = 0; i < pb; i++) {
			TestFailoverTopology.TestFailoverVertex v = topologyBuilder.newVertex();
			bSet.add(v);
		}

		int pc = pa;
		HashSet<TestFailoverTopology.TestFailoverVertex> cSet = new HashSet<>();
		for (int i = 0; i < pc; i++) {
			TestFailoverTopology.TestFailoverVertex v = topologyBuilder.newVertex();
			cSet.add(v);
		}

		int pd = pa;
		HashSet<TestFailoverTopology.TestFailoverVertex> dSet = new HashSet<>();
		for (int i = 0; i < pd; i++) {
			TestFailoverTopology.TestFailoverVertex v = topologyBuilder.newVertex();
			dSet.add(v);
		}

		int pe = pa;
		HashSet<TestFailoverTopology.TestFailoverVertex> eSet = new HashSet<>();
		for (int i = 0; i < pe; i++) {
			TestFailoverTopology.TestFailoverVertex v = topologyBuilder.newVertex();
			eSet.add(v);
		}

		for (TestFailoverTopology.TestFailoverVertex source : aSet) {
			IntermediateResultPartitionID partitionID = new IntermediateResultPartitionID();
			for (TestFailoverTopology.TestFailoverVertex target : bSet) {
				topologyBuilder.connect(source, target, ResultPartitionType.BLOCKING, partitionID);
			}
		}

		for (TestFailoverTopology.TestFailoverVertex source : bSet) {
			IntermediateResultPartitionID partitionID = new IntermediateResultPartitionID();
			for (TestFailoverTopology.TestFailoverVertex target : cSet) {
				topologyBuilder.connect(source, target, ResultPartitionType.BLOCKING, partitionID);
			}
		}

		for (TestFailoverTopology.TestFailoverVertex source : cSet) {
			IntermediateResultPartitionID partitionID = new IntermediateResultPartitionID();
			for (TestFailoverTopology.TestFailoverVertex target : dSet) {
				topologyBuilder.connect(source, target, ResultPartitionType.PIPELINED, partitionID);
			}
		}

		for (TestFailoverTopology.TestFailoverVertex source : dSet) {
			IntermediateResultPartitionID partitionID = new IntermediateResultPartitionID();
			for (TestFailoverTopology.TestFailoverVertex target : eSet) {
				topologyBuilder.connect(source, target, ResultPartitionType.BLOCKING, partitionID);
			}
		}

		return topologyBuilder.build();
	}

	@Test
	public void testRegionFailoverPerformance() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();
		ArrayList<ArrayList<TestFailoverTopology.TestFailoverVertex>> graph = new ArrayList<>();
		int level = 16;
		int curLevel = 0;
		for (int i = level; i >= 0; i--, curLevel++) {
			ArrayList<TestFailoverTopology.TestFailoverVertex> jv = new ArrayList<>();
			int currentNum = 1 << i;
			for (int j = 0; j < currentNum; j++) {
				TestFailoverTopology.TestFailoverVertex nv = topologyBuilder.newVertex();
				jv.add(nv);
				if (curLevel != 0) {
					topologyBuilder.connect(graph.get(curLevel - 1).get(j << 1), nv, ResultPartitionType.PIPELINED);
					topologyBuilder.connect(graph.get(curLevel - 1).get((j << 1) + 1), nv, ResultPartitionType.PIPELINED);
				}
			}
			graph.add(jv);
		}
		TestFailoverTopology topology = topologyBuilder.build();

		long time = System.currentTimeMillis();
		for (int i = 0; i < 10; i++) {
			RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);
		}
		System.out.println("cost:" + (System.currentTimeMillis() - time));

	}
}
