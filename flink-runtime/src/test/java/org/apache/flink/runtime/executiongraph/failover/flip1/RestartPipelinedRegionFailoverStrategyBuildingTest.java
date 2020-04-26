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
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

/**
 * Tests the failover region building logic of the {@link RestartPipelinedRegionFailoverStrategy}.
 */
public class RestartPipelinedRegionFailoverStrategyBuildingTest extends TestLogger {

	/**
	 * Tests that validates that a graph with single unconnected vertices works correctly.
	 *
	 * <pre>
	 *     (v1)
	 *
	 *     (v2)
	 *
	 *     (v3)
	 * </pre>
	 */
	@Test
	public void testIndividualVertices() {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex();

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		SchedulingPipelinedRegion r1 = strategy.getFailoverRegion(v1.getId());
		SchedulingPipelinedRegion r2 = strategy.getFailoverRegion(v2.getId());
		SchedulingPipelinedRegion r3 = strategy.getFailoverRegion(v3.getId());

		assertDistinctRegions(r1, r2, r3);
	}

	/**
	 * Tests that validates that embarrassingly parallel chains of vertices work correctly.
	 *
	 * <pre>
	 *     (a1) --> (b1)
	 *
	 *     (a2) --> (b2)
	 *
	 *     (a3) --> (b3)
	 * </pre>
	 */
	@Test
	public void testEmbarrassinglyParallelCase() {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex va1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex va2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex va3 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb3 = topology.newExecutionVertex();

		topology
			.connect(va1, vb1, ResultPartitionType.PIPELINED)
			.connect(va2, vb2, ResultPartitionType.PIPELINED)
			.connect(va3, vb3, ResultPartitionType.PIPELINED);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		SchedulingPipelinedRegion ra1 = strategy.getFailoverRegion(va1.getId());
		SchedulingPipelinedRegion ra2 = strategy.getFailoverRegion(va2.getId());
		SchedulingPipelinedRegion ra3 = strategy.getFailoverRegion(va3.getId());
		SchedulingPipelinedRegion rb1 = strategy.getFailoverRegion(vb1.getId());
		SchedulingPipelinedRegion rb2 = strategy.getFailoverRegion(vb2.getId());
		SchedulingPipelinedRegion rb3 = strategy.getFailoverRegion(vb3.getId());

		assertSameRegion(ra1, rb1);
		assertSameRegion(ra2, rb2);
		assertSameRegion(ra3, rb3);

		assertDistinctRegions(ra1, ra2, ra3);
	}

	/**
	 * Tests that validates that a single pipelined component via a sequence of all-to-all
	 * connections works correctly.
	 *
	 * <pre>
	 *     (a1) -+-> (b1) -+-> (c1)
	 *           X         X
	 *     (a2) -+-> (b2) -+-> (c2)
	 * </pre>
	 */
	@Test
	public void testOneComponentViaTwoExchanges() {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex va1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex va2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vc1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vc2 = topology.newExecutionVertex();

		topology
			.connect(va1, vb1, ResultPartitionType.PIPELINED)
			.connect(va1, vb2, ResultPartitionType.PIPELINED)
			.connect(va2, vb1, ResultPartitionType.PIPELINED)
			.connect(va2, vb2, ResultPartitionType.PIPELINED)
			.connect(vb1, vc1, ResultPartitionType.PIPELINED)
			.connect(vb1, vc2, ResultPartitionType.PIPELINED)
			.connect(vb2, vc1, ResultPartitionType.PIPELINED)
			.connect(vb2, vc2, ResultPartitionType.PIPELINED);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		SchedulingPipelinedRegion ra1 = strategy.getFailoverRegion(va1.getId());
		SchedulingPipelinedRegion ra2 = strategy.getFailoverRegion(va2.getId());
		SchedulingPipelinedRegion rb1 = strategy.getFailoverRegion(vb1.getId());
		SchedulingPipelinedRegion rb2 = strategy.getFailoverRegion(vb2.getId());
		SchedulingPipelinedRegion rc1 = strategy.getFailoverRegion(vc1.getId());
		SchedulingPipelinedRegion rc2 = strategy.getFailoverRegion(vc2.getId());

		assertSameRegion(ra1, ra2, rb1, rb2, rc1, rc2);
	}

	/**
	 * Tests that validates that a single pipelined component via a cascade of joins
	 * works correctly.
	 *
	 * <pre>
	 *     (v1)--+
	 *          +--(v5)-+
	 *     (v2)--+      |
	 *                 +--(v7)
	 *     (v3)--+      |
	 *          +--(v6)-+
	 *     (v4)--+
	 * </pre>
	 */
	@Test
	public void testOneComponentViaCascadeOfJoins() {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v4 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v5 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v6 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v7 = topology.newExecutionVertex();

		topology
			.connect(v1, v5, ResultPartitionType.PIPELINED)
			.connect(v2, v5, ResultPartitionType.PIPELINED)
			.connect(v3, v6, ResultPartitionType.PIPELINED)
			.connect(v4, v6, ResultPartitionType.PIPELINED)
			.connect(v5, v7, ResultPartitionType.PIPELINED)
			.connect(v6, v7, ResultPartitionType.PIPELINED);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		SchedulingPipelinedRegion r1 = strategy.getFailoverRegion(v1.getId());
		SchedulingPipelinedRegion r2 = strategy.getFailoverRegion(v2.getId());
		SchedulingPipelinedRegion r3 = strategy.getFailoverRegion(v3.getId());
		SchedulingPipelinedRegion r4 = strategy.getFailoverRegion(v4.getId());
		SchedulingPipelinedRegion r5 = strategy.getFailoverRegion(v5.getId());
		SchedulingPipelinedRegion r6 = strategy.getFailoverRegion(v6.getId());
		SchedulingPipelinedRegion r7 = strategy.getFailoverRegion(v7.getId());

		assertSameRegion(r1, r2, r3, r4, r5, r6, r7);
	}

	/**
	 * Tests that validates that a single pipelined component instance from one source
	 * works correctly.
	 *
	 * <pre>
	 *                 +--(v4)
	 *          +--(v2)-+
	 *          |      +--(v5)
	 *     (v1)--+
	 *          |      +--(v6)
	 *          +--(v3)-+
	 *                 +--(v7)
	 * </pre>
	 */
	@Test
	public void testOneComponentInstanceFromOneSource() {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v4 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v5 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v6 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v7 = topology.newExecutionVertex();

		topology
			.connect(v1, v2, ResultPartitionType.PIPELINED)
			.connect(v1, v3, ResultPartitionType.PIPELINED)
			.connect(v2, v4, ResultPartitionType.PIPELINED)
			.connect(v2, v5, ResultPartitionType.PIPELINED)
			.connect(v3, v6, ResultPartitionType.PIPELINED)
			.connect(v3, v7, ResultPartitionType.PIPELINED);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		SchedulingPipelinedRegion r1 = strategy.getFailoverRegion(v1.getId());
		SchedulingPipelinedRegion r2 = strategy.getFailoverRegion(v2.getId());
		SchedulingPipelinedRegion r3 = strategy.getFailoverRegion(v3.getId());
		SchedulingPipelinedRegion r4 = strategy.getFailoverRegion(v4.getId());
		SchedulingPipelinedRegion r5 = strategy.getFailoverRegion(v5.getId());
		SchedulingPipelinedRegion r6 = strategy.getFailoverRegion(v6.getId());
		SchedulingPipelinedRegion r7 = strategy.getFailoverRegion(v7.getId());

		assertSameRegion(r1, r2, r3, r4, r5, r6, r7);
	}

	/**
	 * Tests the below topology.
	 * <pre>
	 *     (a1) -+-> (b1) -+-> (c1)
	 *           X
	 *     (a2) -+-> (b2) -+-> (c2)
	 *
	 *           ^         ^
	 *           |         |
	 *     (pipelined) (blocking)
	 * </pre>
	 */
	@Test
	public void testTwoComponentsViaBlockingExchange() {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex va1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex va2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vc1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vc2 = topology.newExecutionVertex();

		topology
			.connect(va1, vb1, ResultPartitionType.PIPELINED)
			.connect(va1, vb2, ResultPartitionType.PIPELINED)
			.connect(va2, vb1, ResultPartitionType.PIPELINED)
			.connect(va2, vb2, ResultPartitionType.PIPELINED)
			.connect(vb1, vc1, ResultPartitionType.BLOCKING)
			.connect(vb2, vc2, ResultPartitionType.BLOCKING);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		SchedulingPipelinedRegion ra1 = strategy.getFailoverRegion(va1.getId());
		SchedulingPipelinedRegion ra2 = strategy.getFailoverRegion(va2.getId());
		SchedulingPipelinedRegion rb1 = strategy.getFailoverRegion(vb1.getId());
		SchedulingPipelinedRegion rb2 = strategy.getFailoverRegion(vb2.getId());
		SchedulingPipelinedRegion rc1 = strategy.getFailoverRegion(vc1.getId());
		SchedulingPipelinedRegion rc2 = strategy.getFailoverRegion(vc2.getId());

		assertSameRegion(ra1, ra2, rb1, rb2);

		assertDistinctRegions(ra1, rc1, rc2);
	}

	/**
	 * Tests the below topology.
	 * <pre>
	 *     (a1) -+-> (b1) -+-> (c1)
	 *           X         X
	 *     (a2) -+-> (b2) -+-> (c2)
	 *
	 *           ^         ^
	 *           |         |
	 *     (pipelined) (blocking)
	 * </pre>
	 */
	@Test
	public void testTwoComponentsViaBlockingExchange2() {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex va1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex va2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vc1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vc2 = topology.newExecutionVertex();

		topology
			.connect(va1, vb1, ResultPartitionType.PIPELINED)
			.connect(va1, vb2, ResultPartitionType.PIPELINED)
			.connect(va2, vb1, ResultPartitionType.PIPELINED)
			.connect(va2, vb2, ResultPartitionType.PIPELINED)
			.connect(vb1, vc1, ResultPartitionType.BLOCKING)
			.connect(vb1, vc2, ResultPartitionType.BLOCKING)
			.connect(vb2, vc1, ResultPartitionType.BLOCKING)
			.connect(vb2, vc2, ResultPartitionType.BLOCKING);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		SchedulingPipelinedRegion ra1 = strategy.getFailoverRegion(va1.getId());
		SchedulingPipelinedRegion ra2 = strategy.getFailoverRegion(va2.getId());
		SchedulingPipelinedRegion rb1 = strategy.getFailoverRegion(vb1.getId());
		SchedulingPipelinedRegion rb2 = strategy.getFailoverRegion(vb2.getId());
		SchedulingPipelinedRegion rc1 = strategy.getFailoverRegion(vc1.getId());
		SchedulingPipelinedRegion rc2 = strategy.getFailoverRegion(vc2.getId());

		assertSameRegion(ra1, ra2, rb1, rb2);

		assertDistinctRegions(ra1, rc1, rc2);
	}

	/**
	 * Cascades of joins with partially blocking, partially pipelined exchanges.
	 * <pre>
	 *     (1)--+
	 *          +--(5)-+
	 *     (2)--+      |
	 *              (blocking)
	 *                 |
	 *                 +--(7)
	 *                 |
	 *              (blocking)
	 *     (3)--+      |
	 *          +--(6)-+
	 *     (4)--+
	 * </pre>
	 *
	 * <p>Component 1: 1, 2, 5; component 2: 3,4,6; component 3: 7
	 */
	@Test
	public void testMultipleComponentsViaCascadeOfJoins() {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v4 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v5 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v6 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v7 = topology.newExecutionVertex();

		topology
			.connect(v1, v5, ResultPartitionType.PIPELINED)
			.connect(v2, v5, ResultPartitionType.PIPELINED)
			.connect(v3, v6, ResultPartitionType.PIPELINED)
			.connect(v4, v6, ResultPartitionType.PIPELINED)
			.connect(v5, v7, ResultPartitionType.BLOCKING)
			.connect(v6, v7, ResultPartitionType.BLOCKING);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		SchedulingPipelinedRegion r1 = strategy.getFailoverRegion(v1.getId());
		SchedulingPipelinedRegion r2 = strategy.getFailoverRegion(v2.getId());
		SchedulingPipelinedRegion r3 = strategy.getFailoverRegion(v3.getId());
		SchedulingPipelinedRegion r4 = strategy.getFailoverRegion(v4.getId());
		SchedulingPipelinedRegion r5 = strategy.getFailoverRegion(v5.getId());
		SchedulingPipelinedRegion r6 = strategy.getFailoverRegion(v6.getId());
		SchedulingPipelinedRegion r7 = strategy.getFailoverRegion(v7.getId());

		assertSameRegion(r1, r2, r5);
		assertSameRegion(r3, r4, r6);

		assertDistinctRegions(r1, r3, r7);
	}

	/**
	 * Tests the below topology.
	 * <pre>
	 *       (blocking)
	 *           |
	 *           v
	 *          +|-(v2)-+
	 *          |       |
	 *     (v1)--+      +--(v4)
	 *          |       |
	 *          +--(v3)-+
	 * </pre>
	 */
	@Test
	public void testDiamondWithMixedPipelinedAndBlockingExchanges() {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex v1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v3 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex v4 = topology.newExecutionVertex();

		topology
			.connect(v1, v2, ResultPartitionType.BLOCKING)
			.connect(v1, v3, ResultPartitionType.PIPELINED)
			.connect(v2, v4, ResultPartitionType.PIPELINED)
			.connect(v3, v4, ResultPartitionType.PIPELINED);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		SchedulingPipelinedRegion r1 = strategy.getFailoverRegion(v1.getId());
		SchedulingPipelinedRegion r2 = strategy.getFailoverRegion(v2.getId());
		SchedulingPipelinedRegion r3 = strategy.getFailoverRegion(v3.getId());
		SchedulingPipelinedRegion r4 = strategy.getFailoverRegion(v4.getId());

		assertSameRegion(r1, r2, r3, r4);
	}

	/**
	 * This test checks that are strictly co-located vertices are in the same failover region,
	 * even through they are only connected via a blocking pattern.
	 * This is currently an assumption / limitation of the scheduler.
	 * <pre>
	 *     (a1) -+-> (b1)
	 *           X
	 *     (a2) -+-> (b2)
	 *
	 *           ^
	 *           |
	 *       (blocking)
	 * </pre>
	 */
	@Test
	public void testBlockingAllToAllTopologyWithCoLocation() {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex va1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex va2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb2 = topology.newExecutionVertex();

		topology
			.connect(va1, vb1, ResultPartitionType.BLOCKING)
			.connect(va1, vb2, ResultPartitionType.BLOCKING)
			.connect(va2, vb1, ResultPartitionType.BLOCKING)
			.connect(va2, vb2, ResultPartitionType.BLOCKING);

		topology.setContainsCoLocationConstraints(true);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		SchedulingPipelinedRegion ra1 = strategy.getFailoverRegion(va1.getId());
		SchedulingPipelinedRegion ra2 = strategy.getFailoverRegion(va2.getId());
		SchedulingPipelinedRegion rb1 = strategy.getFailoverRegion(vb1.getId());
		SchedulingPipelinedRegion rb2 = strategy.getFailoverRegion(vb2.getId());

		assertSameRegion(ra1, ra2, rb1, rb2);
	}

	/**
	 * This test checks that are strictly co-located vertices are in the same failover region,
	 * even through they are not connected.
	 * This is currently an assumption / limitation of the scheduler.
	 * <pre>
	 *     (a1) -+-> (b1)
	 *
	 *     (a2) -+-> (b2)
	 * </pre>
	 */
	@Test
	public void testPipelinedOneToOneTopologyWithCoLocation() {
		TestingSchedulingTopology topology = new TestingSchedulingTopology();

		TestingSchedulingExecutionVertex va1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex va2 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb1 = topology.newExecutionVertex();
		TestingSchedulingExecutionVertex vb2 = topology.newExecutionVertex();

		topology
			.connect(va1, vb1, ResultPartitionType.PIPELINED)
			.connect(va2, vb2, ResultPartitionType.PIPELINED);

		topology.setContainsCoLocationConstraints(true);

		RestartPipelinedRegionFailoverStrategy strategy = new RestartPipelinedRegionFailoverStrategy(topology);

		SchedulingPipelinedRegion ra1 = strategy.getFailoverRegion(va1.getId());
		SchedulingPipelinedRegion ra2 = strategy.getFailoverRegion(va2.getId());
		SchedulingPipelinedRegion rb1 = strategy.getFailoverRegion(vb1.getId());
		SchedulingPipelinedRegion rb2 = strategy.getFailoverRegion(vb2.getId());

		assertSameRegion(ra1, ra2, rb1, rb2);
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	public static void assertSameRegion(SchedulingPipelinedRegion ...regions) {
		checkNotNull(regions);
		for (int i = 0; i < regions.length; i++) {
			for (int j = i + 1; i < regions.length; i++) {
				assertSame(regions[i], regions[j]);
			}
		}
	}

	public static void assertDistinctRegions(SchedulingPipelinedRegion ...regions) {
		checkNotNull(regions);
		for (int i = 0; i < regions.length; i++) {
			for (int j = i + 1; j < regions.length; j++) {
				assertNotSame(regions[i], regions[j]);
			}
		}
	}
}
