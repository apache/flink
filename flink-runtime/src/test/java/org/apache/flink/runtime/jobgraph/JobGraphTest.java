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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupDesc;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link JobGraph}.
 */
public class JobGraphTest extends TestLogger {

	@Test
	public void testSerialization() {
		try {
			JobGraph jg = new JobGraph("The graph");

			// add some configuration values
			{
				jg.getJobConfiguration().setString("some key", "some value");
				jg.getJobConfiguration().setDouble("Life of ", Math.PI);
			}

			// add some vertices
			{
				JobVertex source1 = new JobVertex("source1");
				JobVertex source2 = new JobVertex("source2");
				JobVertex target = new JobVertex("target");
				target.connectNewDataSetAsInput(source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
				target.connectNewDataSetAsInput(source2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

				jg.addVertex(source1);
				jg.addVertex(source2);
				jg.addVertex(target);
			}

			// de-/serialize and compare
			JobGraph copy = CommonTestUtils.createCopySerializable(jg);

			assertEquals(jg.getName(), copy.getName());
			assertEquals(jg.getJobID(), copy.getJobID());
			assertEquals(jg.getJobConfiguration(), copy.getJobConfiguration());
			assertEquals(jg.getNumberOfVertices(), copy.getNumberOfVertices());

			for (JobVertex vertex : copy.getVertices()) {
				JobVertex original = jg.findVertexByID(vertex.getID());
				assertNotNull(original);
				assertEquals(original.getName(), vertex.getName());
				assertEquals(original.getNumberOfInputs(), vertex.getNumberOfInputs());
				assertEquals(original.getNumberOfProducedIntermediateDataSets(), vertex.getNumberOfProducedIntermediateDataSets());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTopologicalSort1() {
		try {
			JobVertex source1 = new JobVertex("source1");
			JobVertex source2 = new JobVertex("source2");
			JobVertex target1 = new JobVertex("target1");
			JobVertex target2 = new JobVertex("target2");
			JobVertex intermediate1 = new JobVertex("intermediate1");
			JobVertex intermediate2 = new JobVertex("intermediate2");

			target1.connectNewDataSetAsInput(source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			target2.connectNewDataSetAsInput(source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			target2.connectNewDataSetAsInput(intermediate2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			intermediate2.connectNewDataSetAsInput(intermediate1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			intermediate1.connectNewDataSetAsInput(source2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

			JobGraph graph = new JobGraph("TestGraph",
				source1, source2, intermediate1, intermediate2, target1, target2);
			List<JobVertex> sorted = graph.getVerticesSortedTopologicallyFromSources();

			assertEquals(6, sorted.size());

			assertBefore(source1, target1, sorted);
			assertBefore(source1, target2, sorted);
			assertBefore(source2, target2, sorted);
			assertBefore(source2, intermediate1, sorted);
			assertBefore(source2, intermediate2, sorted);
			assertBefore(intermediate1, target2, sorted);
			assertBefore(intermediate2, target2, sorted);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTopologicalSort2() {
		try {
			JobVertex source1 = new JobVertex("source1");
			JobVertex source2 = new JobVertex("source2");
			JobVertex root = new JobVertex("root");
			JobVertex l11 = new JobVertex("layer 1 - 1");
			JobVertex l12 = new JobVertex("layer 1 - 2");
			JobVertex l13 = new JobVertex("layer 1 - 3");
			JobVertex l2 = new JobVertex("layer 2");

			root.connectNewDataSetAsInput(l13, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			root.connectNewDataSetAsInput(source2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			root.connectNewDataSetAsInput(l2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

			l2.connectNewDataSetAsInput(l11, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			l2.connectNewDataSetAsInput(l12, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

			l11.connectNewDataSetAsInput(source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

			l12.connectNewDataSetAsInput(source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			l12.connectNewDataSetAsInput(source2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

			l13.connectNewDataSetAsInput(source2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

			JobGraph graph = new JobGraph("TestGraph",
				source1, source2, root, l11, l13, l12, l2);
			List<JobVertex> sorted = graph.getVerticesSortedTopologicallyFromSources();

			assertEquals(7,  sorted.size());

			assertBefore(source1, root, sorted);
			assertBefore(source2, root, sorted);
			assertBefore(l11, root, sorted);
			assertBefore(l12, root, sorted);
			assertBefore(l13, root, sorted);
			assertBefore(l2, root, sorted);

			assertBefore(l11, l2, sorted);
			assertBefore(l12, l2, sorted);
			assertBefore(l2, root, sorted);

			assertBefore(source1, l2, sorted);
			assertBefore(source2, l2, sorted);

			assertBefore(source2, l13, sorted);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTopologicalSort3() {
		//             --> op1 --
		//            /         \
		//  (source) -           +-> op2 -> op3
		//            \         /
		//             ---------

		try {
			JobVertex source = new JobVertex("source");
			JobVertex op1 = new JobVertex("op4");
			JobVertex op2 = new JobVertex("op2");
			JobVertex op3 = new JobVertex("op3");

			op1.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			op2.connectNewDataSetAsInput(op1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			op2.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			op3.connectNewDataSetAsInput(op2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

			JobGraph graph = new JobGraph("TestGraph", source, op1, op2, op3);
			List<JobVertex> sorted = graph.getVerticesSortedTopologicallyFromSources();

			assertEquals(4,  sorted.size());

			assertBefore(source, op1, sorted);
			assertBefore(source, op2, sorted);
			assertBefore(op1, op2, sorted);
			assertBefore(op2, op3, sorted);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTopoSortCyclicGraphNoSources() {
		try {
			JobVertex v1 = new JobVertex("1");
			JobVertex v2 = new JobVertex("2");
			JobVertex v3 = new JobVertex("3");
			JobVertex v4 = new JobVertex("4");

			v1.connectNewDataSetAsInput(v4, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			v3.connectNewDataSetAsInput(v2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			v4.connectNewDataSetAsInput(v3, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

			JobGraph jg = new JobGraph("Cyclic Graph", v1, v2, v3, v4);
			try {
				jg.getVerticesSortedTopologicallyFromSources();
				fail("Failed to raise error on topologically sorting cyclic graph.");
			}
			catch (InvalidProgramException e) {
				// that what we wanted
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTopoSortCyclicGraphIntermediateCycle() {
		try {
			JobVertex source = new JobVertex("source");
			JobVertex v1 = new JobVertex("1");
			JobVertex v2 = new JobVertex("2");
			JobVertex v3 = new JobVertex("3");
			JobVertex v4 = new JobVertex("4");
			JobVertex target = new JobVertex("target");

			v1.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			v1.connectNewDataSetAsInput(v4, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			v3.connectNewDataSetAsInput(v2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			v4.connectNewDataSetAsInput(v3, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
			target.connectNewDataSetAsInput(v3, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

			JobGraph jg = new JobGraph("Cyclic Graph", v1, v2, v3, v4, source, target);
			try {
				jg.getVerticesSortedTopologicallyFromSources();
				fail("Failed to raise error on topologically sorting cyclic graph.");
			}
			catch (InvalidProgramException e) {
				// that what we wanted
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static final void assertBefore(JobVertex v1, JobVertex v2, List<JobVertex> list) {
		boolean seenFirst = false;
		for (JobVertex v : list) {
			if (v == v1) {
				seenFirst = true;
			}
			else if (v == v2) {
				if (!seenFirst) {
					fail("The first vertex (" + v1 + ") is not before the second vertex (" + v2 + ")");
				}
				break;
			}
		}
	}

	@Test
	public void testSetUserArtifactBlobKey() throws IOException, ClassNotFoundException {
		JobGraph jb = new JobGraph();

		final DistributedCache.DistributedCacheEntry[] entries = {
			new DistributedCache.DistributedCacheEntry("p1", true, true),
			new DistributedCache.DistributedCacheEntry("p2", true, false),
			new DistributedCache.DistributedCacheEntry("p3", false, true),
			new DistributedCache.DistributedCacheEntry("p4", true, false),
		};

		for (DistributedCache.DistributedCacheEntry entry : entries) {
			jb.addUserArtifact(entry.filePath, entry);
		}

		for (DistributedCache.DistributedCacheEntry entry : entries) {
			PermanentBlobKey blobKey = new PermanentBlobKey();
			jb.setUserArtifactBlobKey(entry.filePath, blobKey);

			DistributedCache.DistributedCacheEntry jobGraphEntry = jb.getUserArtifacts().get(entry.filePath);
			assertNotNull(jobGraphEntry);
			assertEquals(blobKey, InstantiationUtil.deserializeObject(jobGraphEntry.blobKey, ClassLoader.getSystemClassLoader(), false));
			assertEquals(entry.isExecutable, jobGraphEntry.isExecutable);
			assertEquals(entry.isZipped, jobGraphEntry.isZipped);
			assertEquals(entry.filePath, jobGraphEntry.filePath);
		}
	}

	@Test
	public void checkpointingIsDisabledByDefault() {
		final JobGraph jobGraph = new JobGraph();

		assertFalse(jobGraph.isCheckpointingEnabled());
	}

	@Test
	public void checkpointingIsEnabledIfIntervalIsqAndLegal() {
		final JobGraph jobGraph = new JobGraph();
		jobGraph.setSnapshotSettings(createCheckpointSettingsWithInterval(10));

		assertTrue(jobGraph.isCheckpointingEnabled());
	}

	@Test
	public void checkpointingIsDisabledIfIntervalIsMaxValue() {
		final JobGraph jobGraph = new JobGraph();
		jobGraph.setSnapshotSettings(createCheckpointSettingsWithInterval(Long.MAX_VALUE));

		assertFalse(jobGraph.isCheckpointingEnabled());
	}

	private static JobCheckpointingSettings createCheckpointSettingsWithInterval(final long checkpointInterval) {
		final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration = new CheckpointCoordinatorConfiguration(
			checkpointInterval,
			Long.MAX_VALUE,
			Long.MAX_VALUE,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			false,
			0);

		return new JobCheckpointingSettings(
			Collections.emptyList(),
			Collections.emptyList(),
			Collections.emptyList(),
			checkpointCoordinatorConfiguration,
			null);
	}

	@Test
	public void testGetSlotSharingGroups() {
		final JobVertex v1 = new JobVertex("1");
		final JobVertex v2 = new JobVertex("2");
		final JobVertex v3 = new JobVertex("3");
		final JobVertex v4 = new JobVertex("4");

		final SlotSharingGroup group1 = new SlotSharingGroup();
		v1.setSlotSharingGroup(group1);
		v2.setSlotSharingGroup(group1);

		final SlotSharingGroup group2 = new SlotSharingGroup();
		v3.setSlotSharingGroup(group2);
		v4.setSlotSharingGroup(group2);

		final JobGraph jobGraph = new JobGraph(v1, v2, v3, v4);

		assertThat(jobGraph.getSlotSharingGroups(), containsInAnyOrder(group1, group2));
	}

	@Test
	public void testGetCoLocationGroupDescriptors() {
		final JobVertex v1 = new JobVertex("1");
		final JobVertex v2 = new JobVertex("2");
		final JobVertex v3 = new JobVertex("3");
		final JobVertex v4 = new JobVertex("4");

		final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
		v1.setSlotSharingGroup(slotSharingGroup);
		v2.setSlotSharingGroup(slotSharingGroup);
		v1.setStrictlyCoLocatedWith(v2);

		final JobGraph jobGraph = new JobGraph(v1, v2, v3, v4);

		assertThat(jobGraph.getCoLocationGroupDescriptors(), hasSize(1));

		final CoLocationGroupDesc onlyCoLocationGroupDesc = jobGraph.getCoLocationGroupDescriptors().iterator().next();
		assertThat(onlyCoLocationGroupDesc.getVertices(), containsInAnyOrder(v1.getID(), v2.getID()));
	}
}
