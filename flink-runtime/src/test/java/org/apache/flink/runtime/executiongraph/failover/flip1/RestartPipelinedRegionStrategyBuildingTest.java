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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

/**
 * Tests the failover region building logic of the {@link RestartPipelinedRegionStrategy}.
 */
public class RestartPipelinedRegionStrategyBuildingTest extends TestLogger {

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
	public void testIndividualVertices() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		TestFailoverTopology.TestFailoverVertex v1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v3 = topologyBuilder.newVertex();

		FailoverTopology topology = topologyBuilder.build();

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);

		FailoverRegion r1 = strategy.getFailoverRegion(v1.getExecutionVertexID());
		FailoverRegion r2 = strategy.getFailoverRegion(v2.getExecutionVertexID());
		FailoverRegion r3 = strategy.getFailoverRegion(v3.getExecutionVertexID());

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
	public void testEmbarrassinglyParallelCase() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		TestFailoverTopology.TestFailoverVertex va1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex va2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex va3 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb3 = topologyBuilder.newVertex();

		topologyBuilder
			.connect(va1, vb1, ResultPartitionType.PIPELINED)
			.connect(va2, vb2, ResultPartitionType.PIPELINED)
			.connect(va3, vb3, ResultPartitionType.PIPELINED);

		FailoverTopology topology = topologyBuilder.build();

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);

		FailoverRegion ra1 = strategy.getFailoverRegion(va1.getExecutionVertexID());
		FailoverRegion ra2 = strategy.getFailoverRegion(va2.getExecutionVertexID());
		FailoverRegion ra3 = strategy.getFailoverRegion(va3.getExecutionVertexID());
		FailoverRegion rb1 = strategy.getFailoverRegion(vb1.getExecutionVertexID());
		FailoverRegion rb2 = strategy.getFailoverRegion(vb2.getExecutionVertexID());
		FailoverRegion rb3 = strategy.getFailoverRegion(vb3.getExecutionVertexID());

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
	public void testOneComponentViaTwoExchanges() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		TestFailoverTopology.TestFailoverVertex va1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex va2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vc1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vc2 = topologyBuilder.newVertex();

		topologyBuilder
			.connect(va1, vb1, ResultPartitionType.PIPELINED)
			.connect(va1, vb2, ResultPartitionType.PIPELINED)
			.connect(va2, vb1, ResultPartitionType.PIPELINED)
			.connect(va2, vb2, ResultPartitionType.PIPELINED)
			.connect(vb1, vc1, ResultPartitionType.PIPELINED)
			.connect(vb1, vc2, ResultPartitionType.PIPELINED)
			.connect(vb2, vc1, ResultPartitionType.PIPELINED)
			.connect(vb2, vc2, ResultPartitionType.PIPELINED);

		FailoverTopology topology = topologyBuilder.build();

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);

		FailoverRegion ra1 = strategy.getFailoverRegion(va1.getExecutionVertexID());
		FailoverRegion ra2 = strategy.getFailoverRegion(va2.getExecutionVertexID());
		FailoverRegion rb1 = strategy.getFailoverRegion(vb1.getExecutionVertexID());
		FailoverRegion rb2 = strategy.getFailoverRegion(vb2.getExecutionVertexID());
		FailoverRegion rc1 = strategy.getFailoverRegion(vc1.getExecutionVertexID());
		FailoverRegion rc2 = strategy.getFailoverRegion(vc2.getExecutionVertexID());

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
	public void testOneComponentViaCascadeOfJoins() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		TestFailoverTopology.TestFailoverVertex v1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v3 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v4 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v5 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v6 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v7 = topologyBuilder.newVertex();

		topologyBuilder
			.connect(v1, v5, ResultPartitionType.PIPELINED)
			.connect(v2, v5, ResultPartitionType.PIPELINED)
			.connect(v3, v6, ResultPartitionType.PIPELINED)
			.connect(v4, v6, ResultPartitionType.PIPELINED)
			.connect(v5, v7, ResultPartitionType.PIPELINED)
			.connect(v6, v7, ResultPartitionType.PIPELINED);

		FailoverTopology topology = topologyBuilder.build();

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);

		FailoverRegion r1 = strategy.getFailoverRegion(v1.getExecutionVertexID());
		FailoverRegion r2 = strategy.getFailoverRegion(v2.getExecutionVertexID());
		FailoverRegion r3 = strategy.getFailoverRegion(v3.getExecutionVertexID());
		FailoverRegion r4 = strategy.getFailoverRegion(v4.getExecutionVertexID());
		FailoverRegion r5 = strategy.getFailoverRegion(v5.getExecutionVertexID());
		FailoverRegion r6 = strategy.getFailoverRegion(v6.getExecutionVertexID());
		FailoverRegion r7 = strategy.getFailoverRegion(v7.getExecutionVertexID());

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
	public void testOneComponentInstanceFromOneSource() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		TestFailoverTopology.TestFailoverVertex v1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v3 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v4 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v5 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v6 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v7 = topologyBuilder.newVertex();

		topologyBuilder
			.connect(v1, v2, ResultPartitionType.PIPELINED)
			.connect(v1, v3, ResultPartitionType.PIPELINED)
			.connect(v2, v4, ResultPartitionType.PIPELINED)
			.connect(v2, v5, ResultPartitionType.PIPELINED)
			.connect(v3, v6, ResultPartitionType.PIPELINED)
			.connect(v3, v7, ResultPartitionType.PIPELINED);

		FailoverTopology topology = topologyBuilder.build();

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);

		FailoverRegion r1 = strategy.getFailoverRegion(v1.getExecutionVertexID());
		FailoverRegion r2 = strategy.getFailoverRegion(v2.getExecutionVertexID());
		FailoverRegion r3 = strategy.getFailoverRegion(v3.getExecutionVertexID());
		FailoverRegion r4 = strategy.getFailoverRegion(v4.getExecutionVertexID());
		FailoverRegion r5 = strategy.getFailoverRegion(v5.getExecutionVertexID());
		FailoverRegion r6 = strategy.getFailoverRegion(v6.getExecutionVertexID());
		FailoverRegion r7 = strategy.getFailoverRegion(v7.getExecutionVertexID());

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
	public void testTwoComponentsViaBlockingExchange() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		TestFailoverTopology.TestFailoverVertex va1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex va2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vc1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vc2 = topologyBuilder.newVertex();

		topologyBuilder
			.connect(va1, vb1, ResultPartitionType.PIPELINED)
			.connect(va1, vb2, ResultPartitionType.PIPELINED)
			.connect(va2, vb1, ResultPartitionType.PIPELINED)
			.connect(va2, vb2, ResultPartitionType.PIPELINED)
			.connect(vb1, vc1, ResultPartitionType.BLOCKING)
			.connect(vb2, vc2, ResultPartitionType.BLOCKING);

		FailoverTopology topology = topologyBuilder.build();

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);

		FailoverRegion ra1 = strategy.getFailoverRegion(va1.getExecutionVertexID());
		FailoverRegion ra2 = strategy.getFailoverRegion(va2.getExecutionVertexID());
		FailoverRegion rb1 = strategy.getFailoverRegion(vb1.getExecutionVertexID());
		FailoverRegion rb2 = strategy.getFailoverRegion(vb2.getExecutionVertexID());
		FailoverRegion rc1 = strategy.getFailoverRegion(vc1.getExecutionVertexID());
		FailoverRegion rc2 = strategy.getFailoverRegion(vc2.getExecutionVertexID());

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
	public void testTwoComponentsViaBlockingExchange2() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		TestFailoverTopology.TestFailoverVertex va1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex va2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vc1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vc2 = topologyBuilder.newVertex();

		topologyBuilder
			.connect(va1, vb1, ResultPartitionType.PIPELINED)
			.connect(va1, vb2, ResultPartitionType.PIPELINED)
			.connect(va2, vb1, ResultPartitionType.PIPELINED)
			.connect(va2, vb2, ResultPartitionType.PIPELINED)
			.connect(vb1, vc1, ResultPartitionType.BLOCKING)
			.connect(vb1, vc2, ResultPartitionType.BLOCKING)
			.connect(vb2, vc1, ResultPartitionType.BLOCKING)
			.connect(vb2, vc2, ResultPartitionType.BLOCKING);

		FailoverTopology topology = topologyBuilder.build();

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);

		FailoverRegion ra1 = strategy.getFailoverRegion(va1.getExecutionVertexID());
		FailoverRegion ra2 = strategy.getFailoverRegion(va2.getExecutionVertexID());
		FailoverRegion rb1 = strategy.getFailoverRegion(vb1.getExecutionVertexID());
		FailoverRegion rb2 = strategy.getFailoverRegion(vb2.getExecutionVertexID());
		FailoverRegion rc1 = strategy.getFailoverRegion(vc1.getExecutionVertexID());
		FailoverRegion rc2 = strategy.getFailoverRegion(vc2.getExecutionVertexID());

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
	public void testMultipleComponentsViaCascadeOfJoins() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		TestFailoverTopology.TestFailoverVertex v1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v3 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v4 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v5 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v6 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v7 = topologyBuilder.newVertex();

		topologyBuilder
			.connect(v1, v5, ResultPartitionType.PIPELINED)
			.connect(v2, v5, ResultPartitionType.PIPELINED)
			.connect(v3, v6, ResultPartitionType.PIPELINED)
			.connect(v4, v6, ResultPartitionType.PIPELINED)
			.connect(v5, v7, ResultPartitionType.BLOCKING)
			.connect(v6, v7, ResultPartitionType.BLOCKING);

		FailoverTopology topology = topologyBuilder.build();

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);

		FailoverRegion r1 = strategy.getFailoverRegion(v1.getExecutionVertexID());
		FailoverRegion r2 = strategy.getFailoverRegion(v2.getExecutionVertexID());
		FailoverRegion r3 = strategy.getFailoverRegion(v3.getExecutionVertexID());
		FailoverRegion r4 = strategy.getFailoverRegion(v4.getExecutionVertexID());
		FailoverRegion r5 = strategy.getFailoverRegion(v5.getExecutionVertexID());
		FailoverRegion r6 = strategy.getFailoverRegion(v6.getExecutionVertexID());
		FailoverRegion r7 = strategy.getFailoverRegion(v7.getExecutionVertexID());

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
	public void testDiamondWithMixedPipelinedAndBlockingExchanges() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		TestFailoverTopology.TestFailoverVertex v1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v3 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex v4 = topologyBuilder.newVertex();

		topologyBuilder
			.connect(v1, v2, ResultPartitionType.BLOCKING)
			.connect(v1, v3, ResultPartitionType.PIPELINED)
			.connect(v2, v4, ResultPartitionType.PIPELINED)
			.connect(v3, v4, ResultPartitionType.PIPELINED);

		FailoverTopology topology = topologyBuilder.build();

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);

		FailoverRegion r1 = strategy.getFailoverRegion(v1.getExecutionVertexID());
		FailoverRegion r2 = strategy.getFailoverRegion(v2.getExecutionVertexID());
		FailoverRegion r3 = strategy.getFailoverRegion(v3.getExecutionVertexID());
		FailoverRegion r4 = strategy.getFailoverRegion(v4.getExecutionVertexID());

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
	public void testBlockingAllToAllTopologyWithCoLocation() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		TestFailoverTopology.TestFailoverVertex va1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex va2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb2 = topologyBuilder.newVertex();

		topologyBuilder
			.connect(va1, vb1, ResultPartitionType.BLOCKING)
			.connect(va1, vb2, ResultPartitionType.BLOCKING)
			.connect(va2, vb1, ResultPartitionType.BLOCKING)
			.connect(va2, vb2, ResultPartitionType.BLOCKING);

		topologyBuilder.setContainsCoLocationConstraints(true);
		FailoverTopology topology = topologyBuilder.build();

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);

		FailoverRegion ra1 = strategy.getFailoverRegion(va1.getExecutionVertexID());
		FailoverRegion ra2 = strategy.getFailoverRegion(va2.getExecutionVertexID());
		FailoverRegion rb1 = strategy.getFailoverRegion(vb1.getExecutionVertexID());
		FailoverRegion rb2 = strategy.getFailoverRegion(vb2.getExecutionVertexID());

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
	public void testPipelinedOneToOneTopologyWithCoLocation() throws Exception {
		TestFailoverTopology.Builder topologyBuilder = new TestFailoverTopology.Builder();

		TestFailoverTopology.TestFailoverVertex va1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex va2 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb1 = topologyBuilder.newVertex();
		TestFailoverTopology.TestFailoverVertex vb2 = topologyBuilder.newVertex();

		topologyBuilder
			.connect(va1, vb1, ResultPartitionType.PIPELINED)
			.connect(va2, vb2, ResultPartitionType.PIPELINED);

		topologyBuilder.setContainsCoLocationConstraints(true);
		FailoverTopology topology = topologyBuilder.build();

		RestartPipelinedRegionStrategy strategy = new RestartPipelinedRegionStrategy(topology);

		FailoverRegion ra1 = strategy.getFailoverRegion(va1.getExecutionVertexID());
		FailoverRegion ra2 = strategy.getFailoverRegion(va2.getExecutionVertexID());
		FailoverRegion rb1 = strategy.getFailoverRegion(vb1.getExecutionVertexID());
		FailoverRegion rb2 = strategy.getFailoverRegion(vb2.getExecutionVertexID());

		assertSameRegion(ra1, ra2, rb1, rb2);
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	public static void assertSameRegion(FailoverRegion ...regions) {
		checkNotNull(regions);
		for (int i = 0; i < regions.length; i++) {
			for (int j = i + 1; i < regions.length; i++) {
				assertSame(regions[i], regions[j]);
			}
		}
	}

	public static void assertDistinctRegions(FailoverRegion ...regions) {
		checkNotNull(regions);
		for (int i = 0; i < regions.length; i++) {
			for (int j = i + 1; j < regions.length; j++) {
				assertNotSame(regions[i], regions[j]);
			}
		}
	}
}
