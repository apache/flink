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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.topology.DefaultLogicalResultTest.assertResultsEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for {@link DefaultLogicalVertex}.
 */
public class DefaultLogicalVertexTest extends TestLogger {

	private JobVertex jobVertex;

	private DefaultLogicalVertex logicalVertex;

	private Map<IntermediateDataSetID, IntermediateDataSet> resultMap;

	private Set<IntermediateDataSet> consumedResults;

	private Set<IntermediateDataSet> producedResults;

	@Before
	public void setUp() throws Exception {
		buildVerticesAndResults();

		logicalVertex = new DefaultLogicalVertex(
			jobVertex,
			rid -> new DefaultLogicalResult(resultMap.get(rid), vid -> null));
	}

	@Test
	public void testConstructor() {
		assertVertexInfoEquals(jobVertex, logicalVertex);
	}

	@Test
	public void testGetConsumedResults() {
		assertResultsEquals(consumedResults, logicalVertex.getConsumedResults());
	}

	@Test
	public void testGetProducedResults() {
		assertResultsEquals(producedResults, logicalVertex.getProducedResults());
	}

	private void buildVerticesAndResults() {
		resultMap = new HashMap<>();
		producedResults = new HashSet<>();
		consumedResults = new HashSet<>();

		final int parallelism = 3;
		jobVertex = createNoOpVertex(parallelism);

		for (int i = 0; i < 5; i++) {
			final IntermediateDataSet producedDataSet = jobVertex.createAndAddResultDataSet(BLOCKING);
			producedResults.add(producedDataSet);
			resultMap.put(producedDataSet.getId(), producedDataSet);
		}

		final JobVertex upStreamJobVertex = createNoOpVertex(parallelism);
		for (int i = 0; i < 5; i++) {
			final IntermediateDataSet consumedDataSet = upStreamJobVertex.createAndAddResultDataSet(PIPELINED);
			jobVertex.connectDataSetAsInput(consumedDataSet, ALL_TO_ALL);
			consumedResults.add(consumedDataSet);
			resultMap.put(consumedDataSet.getId(), consumedDataSet);
		}
	}

	static void assertVerticesEquals(
		final Iterable<JobVertex> jobVertices,
		final Iterable<DefaultLogicalVertex> logicalVertices) {

		final Map<JobVertexID, DefaultLogicalVertex> logicalVertextMap = IterableUtils
			.toStream(logicalVertices)
			.collect(Collectors.toMap(DefaultLogicalVertex::getId, Function.identity()));

		for (JobVertex jobVertex : jobVertices) {
			final DefaultLogicalVertex logicalVertex = logicalVertextMap.remove(jobVertex.getID());

			assertNotNull(logicalVertex);
			assertVertexInfoEquals(jobVertex, logicalVertex);
		}

		// this ensures the two collections exactly matches
		assertEquals(0, logicalVertextMap.size());
	}

	static void assertVertexInfoEquals(
		final JobVertex jobVertex,
		final DefaultLogicalVertex logicalVertex) {

		assertEquals(jobVertex.getID(), logicalVertex.getId());
	}
}
