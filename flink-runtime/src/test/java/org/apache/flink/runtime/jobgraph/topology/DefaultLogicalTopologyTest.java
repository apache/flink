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

package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.topology.DefaultLogicalResultTest.assertResultsEquals;
import static org.apache.flink.runtime.jobgraph.topology.DefaultLogicalVertexTest.assertVertexInfoEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Unit tests for {@link DefaultLogicalTopology}.
 */
public class DefaultLogicalTopologyTest extends TestLogger {

	private JobGraph jobGraph;

	private DefaultLogicalTopology logicalTopology;

	@Before
	public void setUp() throws Exception {
		jobGraph = createJobGraph(false);
		logicalTopology = new DefaultLogicalTopology(jobGraph);
	}

	@Test
	public void testGetVertices() {
		// vertices from getVertices() should be topologically sorted
		final Iterable<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
		final Iterable<DefaultLogicalVertex> logicalVertices = logicalTopology.getVertices();

		assertEquals(Iterables.size(jobVertices), Iterables.size(logicalVertices));

		final Iterator<JobVertex> jobVertexIterator = jobVertices.iterator();
		final Iterator<DefaultLogicalVertex> logicalVertexIterator = logicalVertices.iterator();
		while (jobVertexIterator.hasNext()) {
			assertVertexAndConnectedResultsEquals(jobVertexIterator.next(), logicalVertexIterator.next());
		}
	}

	@Test
	public void testWithCoLocationConstraints() {
		final DefaultLogicalTopology topology = new DefaultLogicalTopology(createJobGraph(true));
		assertTrue(topology.containsCoLocationConstraints());
	}

	@Test
	public void testWithoutCoLocationConstraints() {
		assertFalse(logicalTopology.containsCoLocationConstraints());
	}

	@Test
	public void testGetLogicalPipelinedRegions() {
		final Set<LogicalPipelinedRegion> regions = logicalTopology.getLogicalPipelinedRegions();
		assertEquals(2, regions.size());
	}

	private JobGraph createJobGraph(final boolean containsCoLocationConstraint) {
		final JobVertex[] jobVertices = new JobVertex[3];
		final int parallelism = 3;
		jobVertices[0] = createNoOpVertex("v1", parallelism);
		jobVertices[1] = createNoOpVertex("v2", parallelism);
		jobVertices[2] = createNoOpVertex("v3", parallelism);
		jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, PIPELINED);
		jobVertices[2].connectNewDataSetAsInput(jobVertices[1], ALL_TO_ALL, BLOCKING);

		if (containsCoLocationConstraint) {
			final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
			jobVertices[0].setSlotSharingGroup(slotSharingGroup);
			jobVertices[1].setSlotSharingGroup(slotSharingGroup);

			final CoLocationGroup coLocationGroup = new CoLocationGroup();
			coLocationGroup.addVertex(jobVertices[0]);
			coLocationGroup.addVertex(jobVertices[1]);
			jobVertices[0].updateCoLocationGroup(coLocationGroup);
			jobVertices[1].updateCoLocationGroup(coLocationGroup);
		}

		return new JobGraph(jobVertices);
	}

	private static void assertVertexAndConnectedResultsEquals(
		final JobVertex jobVertex,
		final DefaultLogicalVertex logicalVertex) {

		assertVertexInfoEquals(jobVertex, logicalVertex);

		final List<IntermediateDataSet> consumedResults = jobVertex.getInputs().stream()
			.map(JobEdge::getSource)
			.collect(Collectors.toList());
		assertResultsEquals(consumedResults, logicalVertex.getConsumedResults());

		final List<IntermediateDataSet> producedResults = jobVertex.getProducedDataSets();
		assertResultsEquals(producedResults, logicalVertex.getProducedResults());
	}
}
