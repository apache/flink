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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupDesc;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link LocalInputPreferredSlotSharingStrategy}.
 */
public class LocalInputPreferredSlotSharingStrategyTest extends TestLogger {

	private TestingSchedulingTopology topology;

	private static final JobVertexID JOB_VERTEX_ID_1 = new JobVertexID();
	private static final JobVertexID JOB_VERTEX_ID_2 = new JobVertexID();

	private TestingSchedulingExecutionVertex ev11;
	private TestingSchedulingExecutionVertex ev12;
	private TestingSchedulingExecutionVertex ev21;
	private TestingSchedulingExecutionVertex ev22;

	private Set<SlotSharingGroup> slotSharingGroups;

	@Before
	public void setUp() throws Exception {
		topology = new TestingSchedulingTopology();

		ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
		ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);

		ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
		ev22 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);

		final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
		slotSharingGroup.addVertexToGroup(JOB_VERTEX_ID_1, ResourceSpec.UNKNOWN);
		slotSharingGroup.addVertexToGroup(JOB_VERTEX_ID_2, ResourceSpec.UNKNOWN);
		slotSharingGroups = Collections.singleton(slotSharingGroup);
	}

	@Test
	public void testCoLocationConstraintIsRespected() {
		topology.connect(ev11, ev22);
		topology.connect(ev12, ev21);

		final CoLocationGroupDesc coLocationGroup = CoLocationGroupDesc.from(JOB_VERTEX_ID_1, JOB_VERTEX_ID_2);
		final Set<CoLocationGroupDesc> coLocationGroups = Collections.singleton(coLocationGroup);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
			topology,
			slotSharingGroups,
			coLocationGroups);

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(2));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev11.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev11.getId(), ev21.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev12.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev12.getId(), ev22.getId()));
	}

	@Test
	public void testInputLocalityIsRespected() {
		final TestingSchedulingTopology topology = new TestingSchedulingTopology();

		final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
		final TestingSchedulingExecutionVertex ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);

		final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
		final TestingSchedulingExecutionVertex ev22 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);
		final TestingSchedulingExecutionVertex ev23 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 2);

		topology.connect(ev11, ev21);
		topology.connect(ev11, ev22);
		topology.connect(ev12, ev23);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
			topology,
			slotSharingGroups,
			Collections.emptySet());

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(3));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev21.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev11.getId(), ev21.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev22.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev22.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev23.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev12.getId(), ev23.getId()));
	}

	@Test
	public void testDisjointVerticesInOneGroup() {
		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
			topology,
			slotSharingGroups,
			Collections.emptySet());

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(2));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev11.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev11.getId(), ev21.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev12.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev12.getId(), ev22.getId()));
	}

	@Test
	public void testVerticesInDifferentSlotSharingGroups() {
		final SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
		slotSharingGroup1.addVertexToGroup(JOB_VERTEX_ID_1, ResourceSpec.UNKNOWN);
		final SlotSharingGroup slotSharingGroup2 = new SlotSharingGroup();
		slotSharingGroup2.addVertexToGroup(JOB_VERTEX_ID_2, ResourceSpec.UNKNOWN);

		final Set<SlotSharingGroup> slotSharingGroups = new HashSet<>();
		slotSharingGroups.add(slotSharingGroup1);
		slotSharingGroups.add(slotSharingGroup2);

		final SlotSharingStrategy strategy = new LocalInputPreferredSlotSharingStrategy(
			topology,
			slotSharingGroups,
			Collections.emptySet());

		assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(4));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev11.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev11.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev12.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev12.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev21.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev21.getId()));
		assertThat(
			strategy.getExecutionSlotSharingGroup(ev22.getId()).getExecutionVertexIds(),
			containsInAnyOrder(ev22.getId()));
	}
}
