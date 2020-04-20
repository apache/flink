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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.InputDependencyConstraint.ALL;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link LazyFromSourcesSchedulingStrategy}.
 */
public class LazyFromSourcesSchedulingStrategyTest extends TestLogger {

	private TestingSchedulerOperations testingSchedulerOperation = new TestingSchedulerOperations();

	/**
	 * Tests that when start scheduling lazy from sources scheduling strategy will start input vertices in scheduling topology.
	 */
	@Test
	public void testStartScheduling() {
		final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();

		final List<TestingSchedulingExecutionVertex> producers = testingSchedulingTopology.addExecutionVertices().finish();
		final List<TestingSchedulingExecutionVertex> consumers = testingSchedulingTopology.addExecutionVertices().finish();
		testingSchedulingTopology.connectAllToAll(producers, consumers).finish();

		startScheduling(testingSchedulingTopology);
		assertLatestScheduledVerticesAreEqualTo(producers);
	}

	/**
	 * Tests that when restart tasks will only schedule input ready vertices in given ones.
	 */
	@Test
	public void testRestartBlockingTasks() {
		final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();

		final List<TestingSchedulingExecutionVertex> producers = testingSchedulingTopology.addExecutionVertices().finish();
		final List<TestingSchedulingExecutionVertex> consumers = testingSchedulingTopology.addExecutionVertices().finish();
		testingSchedulingTopology.connectAllToAll(producers, consumers).finish();

		LazyFromSourcesSchedulingStrategy schedulingStrategy = startScheduling(testingSchedulingTopology);

		Set<ExecutionVertexID> verticesToRestart = producers.stream().map(TestingSchedulingExecutionVertex::getId)
			.collect(Collectors.toSet());
		verticesToRestart.addAll(consumers.stream().map(
			TestingSchedulingExecutionVertex::getId).collect(Collectors.toSet()));

		schedulingStrategy.restartTasks(verticesToRestart);
		assertLatestScheduledVerticesAreEqualTo(producers);
	}

	/**
	 * Tests that when restart tasks will schedule input consumable vertices in given ones.
	 */
	@Test
	public void testRestartConsumableBlockingTasks() {
		final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();

		final List<TestingSchedulingExecutionVertex> producers = testingSchedulingTopology.addExecutionVertices().finish();
		final List<TestingSchedulingExecutionVertex> consumers = testingSchedulingTopology.addExecutionVertices().finish();
		testingSchedulingTopology.connectAllToAll(producers, consumers).finish();

		LazyFromSourcesSchedulingStrategy schedulingStrategy = startScheduling(testingSchedulingTopology);

		Set<ExecutionVertexID> verticesToRestart = consumers.stream().map(TestingSchedulingExecutionVertex::getId)
			.collect(Collectors.toSet());

		for (TestingSchedulingExecutionVertex producer : producers) {
			schedulingStrategy.onExecutionStateChange(producer.getId(), ExecutionState.FINISHED);
		}

		schedulingStrategy.restartTasks(verticesToRestart);
		assertLatestScheduledVerticesAreEqualTo(consumers);
	}

	/**
	 * Tests that when all the input partitions are ready will start available downstream {@link ResultPartitionType#BLOCKING} vertices.
	 * vertex#0    vertex#1
	 *       \     /
	 *        \   /
	 *         \ /
	 *  (BLOCKING, ALL)
	 *     vertex#2
	 */
	@Test
	public void testRestartBlockingALLExecutionStateChange() {
		final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();

		final List<TestingSchedulingExecutionVertex> producers1 = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		final List<TestingSchedulingExecutionVertex> producers2 = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		final List<TestingSchedulingExecutionVertex> consumers = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).withInputDependencyConstraint(ALL).finish();
		testingSchedulingTopology.connectPointwise(producers1, consumers).finish();
		testingSchedulingTopology.connectPointwise(producers2, consumers).finish();

		final LazyFromSourcesSchedulingStrategy schedulingStrategy = startScheduling(testingSchedulingTopology);

		for (TestingSchedulingExecutionVertex producer : producers1) {
			schedulingStrategy.onExecutionStateChange(producer.getId(), ExecutionState.FINISHED);
		}
		for (TestingSchedulingExecutionVertex producer : producers2) {
			schedulingStrategy.onExecutionStateChange(producer.getId(), ExecutionState.FINISHED);
		}

		Set<ExecutionVertexID> verticesToRestart = consumers.stream().map(TestingSchedulingExecutionVertex::getId)
			.collect(Collectors.toSet());

		schedulingStrategy.restartTasks(verticesToRestart);
		assertLatestScheduledVerticesAreEqualTo(consumers);
	}

	/**
	 * Tests that when any input dataset finishes will start available downstream {@link ResultPartitionType#BLOCKING} vertices.
	 * vertex#0    vertex#1
	 *       \     /
	 *        \   /
	 *         \ /
	 *  (BLOCKING, ANY)
	 *     vertex#2
	 */
	@Test
	public void testRestartBlockingANYExecutionStateChange() {
		final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();

		final List<TestingSchedulingExecutionVertex> producers1 = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		final List<TestingSchedulingExecutionVertex> producers2 = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		final List<TestingSchedulingExecutionVertex> consumers = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		testingSchedulingTopology.connectPointwise(producers1, consumers).finish();
		testingSchedulingTopology.connectPointwise(producers2, consumers).finish();

		final LazyFromSourcesSchedulingStrategy schedulingStrategy = startScheduling(testingSchedulingTopology);

		for (TestingSchedulingExecutionVertex producer : producers1) {
			schedulingStrategy.onExecutionStateChange(producer.getId(), ExecutionState.FINISHED);
		}

		Set<ExecutionVertexID> verticesToRestart = consumers.stream().map(TestingSchedulingExecutionVertex::getId)
			.collect(Collectors.toSet());

		schedulingStrategy.restartTasks(verticesToRestart);
		assertLatestScheduledVerticesAreEqualTo(consumers);
	}

	/**
	 * Tests that when restart {@link ResultPartitionType#PIPELINED} tasks with {@link ResultPartitionState#CONSUMABLE} will be scheduled.
	 */
	@Test
	public void testRestartConsumablePipelinedTasks() {
		final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();

		final List<TestingSchedulingExecutionVertex> producers = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		final List<TestingSchedulingExecutionVertex> consumers = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		testingSchedulingTopology.connectAllToAll(producers, consumers).withResultPartitionState(ResultPartitionState.CONSUMABLE)
			.withResultPartitionType(PIPELINED).finish();

		LazyFromSourcesSchedulingStrategy schedulingStrategy = startScheduling(testingSchedulingTopology);

		Set<ExecutionVertexID> verticesToRestart = producers.stream().map(TestingSchedulingExecutionVertex::getId)
			.collect(Collectors.toSet());
		verticesToRestart.addAll(consumers.stream().map(
			TestingSchedulingExecutionVertex::getId).collect(Collectors.toList()));

		schedulingStrategy.restartTasks(verticesToRestart);
		List<TestingSchedulingExecutionVertex> toScheduleVertices = new ArrayList<>(producers.size() + consumers.size());
		toScheduleVertices.addAll(producers);
		toScheduleVertices.addAll(consumers);

		assertLatestScheduledVerticesAreEqualTo(toScheduleVertices);
	}

	/**
	 * Tests that when restart {@link ResultPartitionType#PIPELINED} tasks with {@link ResultPartitionState#CREATED} will not be scheduled.
	 */
	@Test
	public void testRestartCreatedPipelinedTasks() {
		final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();

		final List<TestingSchedulingExecutionVertex> producers = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		final List<TestingSchedulingExecutionVertex> consumers = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		testingSchedulingTopology.connectAllToAll(producers, consumers).withResultPartitionState(ResultPartitionState.CREATED)
			.withResultPartitionType(PIPELINED).finish();

		LazyFromSourcesSchedulingStrategy schedulingStrategy = startScheduling(testingSchedulingTopology);

		Set<ExecutionVertexID> verticesToRestart = producers.stream().map(TestingSchedulingExecutionVertex::getId)
			.collect(Collectors.toSet());
		verticesToRestart.addAll(consumers.stream().map(
			TestingSchedulingExecutionVertex::getId).collect(Collectors.toSet()));

		schedulingStrategy.restartTasks(verticesToRestart);
		assertLatestScheduledVerticesAreEqualTo(producers);
	}

	/**
	 * Tests that when partition consumable notified will start available {@link ResultPartitionType#PIPELINED} downstream vertices.
	 */
	@Test
	public void testPipelinedPartitionConsumable() {
		final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();

		final List<TestingSchedulingExecutionVertex> producers = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		final List<TestingSchedulingExecutionVertex> consumers = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		testingSchedulingTopology.connectAllToAll(producers, consumers).withResultPartitionType(PIPELINED).finish();

		final LazyFromSourcesSchedulingStrategy schedulingStrategy = startScheduling(testingSchedulingTopology);

		final TestingSchedulingExecutionVertex producer1 = producers.get(0);
		final TestingSchedulingResultPartition partition1 = producer1.getProducedResults().iterator().next();

		schedulingStrategy.onExecutionStateChange(producer1.getId(), ExecutionState.RUNNING);
		schedulingStrategy.onPartitionConsumable(partition1.getId());

		assertLatestScheduledVerticesAreEqualTo(consumers);
	}

	/**
	 * Tests that when partition consumable notified will start available {@link ResultPartitionType#BLOCKING} downstream vertices.
	 */
	@Test
	public void testBlockingPointwiseExecutionStateChange() {
		final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();

		final List<TestingSchedulingExecutionVertex> producers = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		final List<TestingSchedulingExecutionVertex> consumers = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).withInputDependencyConstraint(ALL).finish();
		testingSchedulingTopology.connectPointwise(producers, consumers).finish();

		final LazyFromSourcesSchedulingStrategy schedulingStrategy = startScheduling(testingSchedulingTopology);

		for (TestingSchedulingExecutionVertex producer : producers) {
			schedulingStrategy.onExecutionStateChange(producer.getId(), ExecutionState.FINISHED);
		}

		assertLatestScheduledVerticesAreEqualTo(consumers);
	}

	/**
	 * Tests that when all the input partitions are ready will start available downstream {@link ResultPartitionType#BLOCKING} vertices.
	 * vertex#0    vertex#1
	 *       \     /
	 *        \   /
	 *         \ /
	 *  (BLOCKING, ALL)
	 *     vertex#2
	 */
	@Test
	public void testBlockingALLExecutionStateChange() {
		final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();

		final List<TestingSchedulingExecutionVertex> producers1 = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		final List<TestingSchedulingExecutionVertex> producers2 = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		final List<TestingSchedulingExecutionVertex> consumers = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).withInputDependencyConstraint(ALL).finish();
		testingSchedulingTopology.connectPointwise(producers1, consumers).finish();
		testingSchedulingTopology.connectPointwise(producers2, consumers).finish();

		final LazyFromSourcesSchedulingStrategy schedulingStrategy = startScheduling(testingSchedulingTopology);

		for (TestingSchedulingExecutionVertex producer : producers1) {
			schedulingStrategy.onExecutionStateChange(producer.getId(), ExecutionState.FINISHED);
		}
		for (TestingSchedulingExecutionVertex producer : producers2) {
			schedulingStrategy.onExecutionStateChange(producer.getId(), ExecutionState.FINISHED);
		}

		assertLatestScheduledVerticesAreEqualTo(consumers);
	}

	/**
	 * Tests that when any input dataset finishes will start available downstream {@link ResultPartitionType#BLOCKING} vertices.
	 * vertex#0    vertex#1
	 *       \     /
	 *        \   /
	 *         \ /
	 *  (BLOCKING, ANY)
	 *     vertex#2
	 */
	@Test
	public void testBlockingANYExecutionStateChange() {
		final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();

		final List<TestingSchedulingExecutionVertex> producers1 = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		final List<TestingSchedulingExecutionVertex> producers2 = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		final List<TestingSchedulingExecutionVertex> consumers = testingSchedulingTopology.addExecutionVertices()
			.withParallelism(2).finish();
		testingSchedulingTopology.connectPointwise(producers1, consumers).finish();
		testingSchedulingTopology.connectPointwise(producers2, consumers).finish();

		final LazyFromSourcesSchedulingStrategy schedulingStrategy = startScheduling(testingSchedulingTopology);

		for (TestingSchedulingExecutionVertex producer : producers1) {
			schedulingStrategy.onExecutionStateChange(producer.getId(), ExecutionState.FINISHED);
		}

		assertLatestScheduledVerticesAreEqualTo(consumers);
	}

	private LazyFromSourcesSchedulingStrategy startScheduling(TestingSchedulingTopology testingSchedulingTopology) {
		LazyFromSourcesSchedulingStrategy schedulingStrategy = new LazyFromSourcesSchedulingStrategy(
			testingSchedulerOperation,
			testingSchedulingTopology);
		schedulingStrategy.startScheduling();
		return schedulingStrategy;
	}

	private void assertLatestScheduledVerticesAreEqualTo(final List<TestingSchedulingExecutionVertex> expected) {
		final List<List<ExecutionVertexDeploymentOption>> deploymentOptions = testingSchedulerOperation.getScheduledVertices();
		assertThat(expected.size(), lessThanOrEqualTo(deploymentOptions.size()));
		for (int i = 0; i < expected.size(); i++) {
			assertEquals(
				idsFromVertices(Collections.singletonList(expected.get(expected.size() - i - 1))),
				idsFromDeploymentOptions(deploymentOptions.get(deploymentOptions.size() - i - 1)));
		}
	}

	private static List<ExecutionVertexID> idsFromVertices(final List<TestingSchedulingExecutionVertex> vertices) {
		return vertices.stream().map(TestingSchedulingExecutionVertex::getId).collect(Collectors.toList());
	}

	private static List<ExecutionVertexID> idsFromDeploymentOptions(
			final List<ExecutionVertexDeploymentOption> deploymentOptions) {

		return deploymentOptions.stream().map(ExecutionVertexDeploymentOption::getExecutionVertexId).collect(Collectors.toList());
	}
}
