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

package org.apache.flink.runtime.schedule;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.event.ExecutionVertexFailoverEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link StepwiseSchedulingPlugin}.
 */
public class StepwiseSchedulingPluginTest extends GraphManagerPluginTestBase {

	/**
	 * Tests stepwise scheduling.
	 */
	@Test
	public void testStepwiseScheduling() throws Exception {

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		v1.setParallelism(3);
		v2.setParallelism(4);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", new JobVertex[] {v1, v2});
		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
			jobGraph,
			new SimpleAckingTaskManagerGateway(),
			new NoRestartStrategy());

		final List<ExecutionVertex> executionVertices = new ArrayList<>();
		final List<ExecutionVertexID> vertices = new ArrayList<>();
		for (ExecutionJobVertex ejv : eg.getVerticesTopologically()) {
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				executionVertices.add(ev);
				vertices.add(ev.getExecutionVertexID());
			}
		}

		final Iterator<ExecutionJobVertex> ejvIterator = eg.getVerticesTopologically().iterator();
		final ExecutionJobVertex ejv1 = ejvIterator.next();
		final List<ExecutionVertexID> vertices1 = new ArrayList<>();
		for (ExecutionVertex ev : ejv1.getTaskVertices()) {
			vertices1.add(ev.getExecutionVertexID());
		}

		final ExecutionJobVertex ejv2 = ejvIterator.next();
		final List<ExecutionVertexID> vertices2 = new ArrayList<>();
		for (ExecutionVertex ev : ejv2.getTaskVertices()) {
			vertices2.add(ev.getExecutionVertexID());
		}

		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(eg, executionVertices);

		final GraphManagerPlugin graphManagerPlugin = new StepwiseSchedulingPlugin();
		graphManagerPlugin.open(
			scheduler,
			jobGraph,
			new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()));

		graphManagerPlugin.onSchedulingStarted();
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices1));
		scheduler.clearScheduledVertices();

		// Set one pipelined partition consumable
		ExecutionJobVertex jv = ejv1;
		for (IntermediateResultPartition partition : eg.getAllVertices().get(jv.getJobVertexId())
			.getTaskVertices()[0].getProducedPartitions().values()) {
			partition.markDataProduced();
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv1.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices2));
		for (IntermediateResultPartition partition : eg.getAllVertices().get(jv.getJobVertexId())
			.getTaskVertices()[0].getProducedPartitions().values()) {
			partition.resetForNewExecution();
		}
		scheduler.clearScheduledVertices();

		graphManagerPlugin.onExecutionVertexFailover(new ExecutionVertexFailoverEvent(vertices));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices1));
	}

	/**
	 * Tests stepwise scheduling with configured input consumable status.
	 * Using default threshold and input constraints.
	 */
	@Test
	public void testStepwiseSchedulingConsideringInputStatusDefault() throws Exception {

		int parallelism = 10;

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		v1.setParallelism(parallelism);
		v2.setParallelism(parallelism);
		v3.setParallelism(parallelism);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", new JobVertex[] {v1, v2, v3});
		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
			jobGraph,
			new SimpleAckingTaskManagerGateway(),
			new NoRestartStrategy());

		final List<ExecutionVertex> executionVertices = new ArrayList<>();
		final List<ExecutionVertexID> vertices = new ArrayList<>();
		for (ExecutionJobVertex ejv : eg.getVerticesTopologically()) {
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				executionVertices.add(ev);
				vertices.add(ev.getExecutionVertexID());
			}
		}

		final Iterator<ExecutionJobVertex> ejvIterator = eg.getVerticesTopologically().iterator();
		final ExecutionJobVertex ejv1 = ejvIterator.next();
		final List<ExecutionVertexID> vertices1 = new ArrayList<>();
		for (ExecutionVertex ev : ejv1.getTaskVertices()) {
			vertices1.add(ev.getExecutionVertexID());
		}

		final ExecutionJobVertex ejv2 = ejvIterator.next();
		final List<ExecutionVertexID> vertices2 = new ArrayList<>();
		for (ExecutionVertex ev : ejv2.getTaskVertices()) {
			vertices2.add(ev.getExecutionVertexID());
		}

		final List<ExecutionVertexID> sourceVertices = new ArrayList<>();
		sourceVertices.addAll(vertices1);
		sourceVertices.addAll(vertices2);

		final ExecutionJobVertex ejv3 = ejvIterator.next();
		final List<ExecutionVertexID> vertices3 = new ArrayList<>();
		for (ExecutionVertex ev : ejv3.getTaskVertices()) {
			vertices3.add(ev.getExecutionVertexID());
		}

		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(eg, executionVertices);

		VertexInputTracker.VertexInputTrackerConfig inputTrackerConfig =
			new VertexInputTracker.VertexInputTrackerConfig(
				VertexInputTracker.InputDependencyConstraint.ANY,
				Double.MIN_VALUE,
				1
			);
		try {
			InstantiationUtil.writeObjectToConfig(inputTrackerConfig, jobGraph.getSchedulingConfiguration(),
				VertexInputTracker.VertexInputTrackerOptions.VERTEX_INPUT_TRACKER_CONFIG);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not serialize job vertex to stream node map", e);
		}

		final GraphManagerPlugin graphManagerPlugin = new StepwiseSchedulingPlugin();
		graphManagerPlugin.open(
			scheduler,
			jobGraph,
			new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()));

		graphManagerPlugin.onSchedulingStarted();
		assertTrue(compareVertices(scheduler.getScheduledVertices(), sourceVertices));
		scheduler.clearScheduledVertices();

		// Set one pipelined partition consumable
		ExecutionJobVertex jv = ejv1;
		for (IntermediateResultPartition partition : eg.getAllVertices().get(jv.getJobVertexId())
			.getTaskVertices()[0].getProducedPartitions().values()) {
			partition.markDataProduced();
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(jv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices3));
		for (IntermediateResultPartition partition : eg.getAllVertices().get(jv.getJobVertexId())
			.getTaskVertices()[0].getProducedPartitions().values()) {
			partition.resetForNewExecution();
		}
		scheduler.clearScheduledVertices();

		// Set one blocking partition consumable
		jv = ejv2;
		for (IntermediateResultPartition partition : eg.getAllVertices().get(jv.getJobVertexId())
			.getTaskVertices()[0].getProducedPartitions().values()) {
			partition.markFinished();
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(jv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), Collections.EMPTY_SET));
		for (IntermediateResultPartition partition : eg.getAllVertices().get(jv.getJobVertexId())
			.getTaskVertices()[0].getProducedPartitions().values()) {
			partition.resetForNewExecution();
		}
		scheduler.clearScheduledVertices();

		// Set all blocking partition consumable
		jv = ejv2;
		for (int i = 0; i < jv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(jv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markFinished();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(jv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices3));
		for (int i = 0; i < jv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(jv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();
	}

	/**
	 * Tests stepwise scheduling with configured input consumable status.
	 * Using strict threshold and input constraints.
	 */
	@Test
	public void testStepwiseSchedulingConsideringInputStatusStrictly() throws Exception {

		int parallelism = 10;

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		v1.setParallelism(parallelism);
		v2.setParallelism(parallelism);
		v3.setParallelism(parallelism);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", new JobVertex[] {v1, v2, v3});
		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
			jobGraph,
			new SimpleAckingTaskManagerGateway(),
			new NoRestartStrategy());

		final List<ExecutionVertex> executionVertices = new ArrayList<>();
		final List<ExecutionVertexID> vertices = new ArrayList<>();
		for (ExecutionJobVertex ejv : eg.getVerticesTopologically()) {
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				executionVertices.add(ev);
				vertices.add(ev.getExecutionVertexID());
			}
		}

		final Iterator<ExecutionJobVertex> ejvIterator = eg.getVerticesTopologically().iterator();
		final ExecutionJobVertex ejv1 = ejvIterator.next();
		final List<ExecutionVertexID> vertices1 = new ArrayList<>();
		for (ExecutionVertex ev : ejv1.getTaskVertices()) {
			vertices1.add(ev.getExecutionVertexID());
		}

		final ExecutionJobVertex ejv2 = ejvIterator.next();
		final List<ExecutionVertexID> vertices2 = new ArrayList<>();
		for (ExecutionVertex ev : ejv2.getTaskVertices()) {
			vertices2.add(ev.getExecutionVertexID());
		}

		final List<ExecutionVertexID> sourceVertices = new ArrayList<>();
		sourceVertices.addAll(vertices1);
		sourceVertices.addAll(vertices2);

		final ExecutionJobVertex ejv3 = ejvIterator.next();
		final List<ExecutionVertexID> vertices3 = new ArrayList<>();
		for (ExecutionVertex ev : ejv3.getTaskVertices()) {
			vertices3.add(ev.getExecutionVertexID());
		}

		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(eg, executionVertices);

		VertexInputTracker.VertexInputTrackerConfig inputTrackerConfig =
			new VertexInputTracker.VertexInputTrackerConfig(
				VertexInputTracker.InputDependencyConstraint.ALL,
				1,
				1
			);
		try {
			InstantiationUtil.writeObjectToConfig(inputTrackerConfig, jobGraph.getSchedulingConfiguration(),
				VertexInputTracker.VertexInputTrackerOptions.VERTEX_INPUT_TRACKER_CONFIG);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not serialize job vertex to stream node map", e);
		}

		final GraphManagerPlugin graphManagerPlugin = new StepwiseSchedulingPlugin();
		graphManagerPlugin.open(
			scheduler,
			jobGraph,
			new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()));

		graphManagerPlugin.onSchedulingStarted();
		assertTrue(compareVertices(scheduler.getScheduledVertices(), sourceVertices));
		scheduler.clearScheduledVertices();

		// Set one pipelined partition consumable
		ExecutionJobVertex ejv = ejv1;
		for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
			.getTaskVertices()[0].getProducedPartitions().values()) {
			partition.markDataProduced();
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), Collections.EMPTY_SET));
		for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
			.getTaskVertices()[0].getProducedPartitions().values()) {
			partition.resetForNewExecution();
		}
		scheduler.clearScheduledVertices();

		// Set all pipelined partition consumable
		ejv = ejv1;
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markDataProduced();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), Collections.EMPTY_SET));
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();

		// Set one blocking partition consumable
		ejv = ejv2;
		for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
			.getTaskVertices()[0].getProducedPartitions().values()) {
			partition.markFinished();
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), Collections.EMPTY_SET));
		for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
			.getTaskVertices()[0].getProducedPartitions().values()) {
			partition.resetForNewExecution();
		}
		scheduler.clearScheduledVertices();

		// Set all blocking partition consumable
		ejv = ejv2;
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markFinished();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), Collections.EMPTY_SET));
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();

		// Set all pipelined and blocking partition consumable
		for (int i = 0; i < ejv1.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv1.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markDataProduced();
			}
		}
		for (int i = 0; i < ejv2.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv2.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markFinished();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices3));
		for (int i = 0; i < ejv1.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv1.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		for (int i = 0; i < ejv2.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv2.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();
	}

	/**
	 * Tests stepwise scheduling with configured input consumable status.
	 * Using percentile threshold and input constraints.
	 */
	@Test
	public void testStepwiseSchedulingConsideringInputStatusPercentile() throws Exception {

		int parallelism = 10;

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		v1.setParallelism(parallelism);
		v2.setParallelism(parallelism);
		v3.setParallelism(parallelism);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", new JobVertex[] {v1, v2, v3});
		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
			jobGraph,
			new SimpleAckingTaskManagerGateway(),
			new NoRestartStrategy());

		final List<ExecutionVertex> executionVertices = new ArrayList<>();
		final List<ExecutionVertexID> vertices = new ArrayList<>();
		for (ExecutionJobVertex ejv : eg.getVerticesTopologically()) {
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				executionVertices.add(ev);
				vertices.add(ev.getExecutionVertexID());
			}
		}

		final Iterator<ExecutionJobVertex> ejvIterator = eg.getVerticesTopologically().iterator();
		final ExecutionJobVertex ejv1 = ejvIterator.next();
		final List<ExecutionVertexID> vertices1 = new ArrayList<>();
		for (ExecutionVertex ev : ejv1.getTaskVertices()) {
			vertices1.add(ev.getExecutionVertexID());
		}

		final ExecutionJobVertex ejv2 = ejvIterator.next();
		final List<ExecutionVertexID> vertices2 = new ArrayList<>();
		for (ExecutionVertex ev : ejv2.getTaskVertices()) {
			vertices2.add(ev.getExecutionVertexID());
		}

		final List<ExecutionVertexID> sourceVertices = new ArrayList<>();
		sourceVertices.addAll(vertices1);
		sourceVertices.addAll(vertices2);

		final ExecutionJobVertex ejv3 = ejvIterator.next();
		final List<ExecutionVertexID> vertices3 = new ArrayList<>();
		for (ExecutionVertex ev : ejv3.getTaskVertices()) {
			vertices3.add(ev.getExecutionVertexID());
		}

		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(eg, executionVertices);

		double pipelinedConsumableThreshold = 0.4;
		double blockingConsumableThreshold = 0.7;
		VertexInputTracker.VertexInputTrackerConfig inputTrackerConfig =
			new VertexInputTracker.VertexInputTrackerConfig(
				VertexInputTracker.InputDependencyConstraint.ANY,
				pipelinedConsumableThreshold,
				blockingConsumableThreshold
			);
		try {
			InstantiationUtil.writeObjectToConfig(inputTrackerConfig, jobGraph.getSchedulingConfiguration(),
				VertexInputTracker.VertexInputTrackerOptions.VERTEX_INPUT_TRACKER_CONFIG);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not serialize job vertex to stream node map", e);
		}

		final GraphManagerPlugin graphManagerPlugin = new StepwiseSchedulingPlugin();
		graphManagerPlugin.open(
			scheduler,
			jobGraph,
			new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()));

		graphManagerPlugin.onSchedulingStarted();
		assertTrue(compareVertices(scheduler.getScheduledVertices(), sourceVertices));
		scheduler.clearScheduledVertices();

		// Set the pipelined result consumable rate smaller than threshold
		ExecutionJobVertex ejv = ejv1;
		for (int i = 0; i < ejv.getParallelism() * pipelinedConsumableThreshold - 1; i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markDataProduced();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), Collections.EMPTY_SET));
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();

		// Set the pipelined result consumable rate larger than threshold
		ejv = ejv1;
		for (int i = 0; i < ejv.getParallelism() * pipelinedConsumableThreshold; i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markDataProduced();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices3));
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();

		// Set the blocking result consumable rate smaller than threshold
		ejv = ejv2;
		for (int i = 0; i < ejv.getParallelism() * blockingConsumableThreshold - 1; i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markFinished();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), Collections.EMPTY_SET));
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();

		// Set the blocking result consumable rate larger than threshold
		ejv = ejv2;
		for (int i = 0; i < ejv.getParallelism() * blockingConsumableThreshold; i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markFinished();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices3));
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();
	}

	/**
	 * Tests stepwise scheduling with configured input consumable status.
	 * Using vertex level percentile threshold and input constraints.
	 */
	@Test
	public void testStepwiseSchedulingConsideringInputStatusVertexLevelPercentile() throws Exception {

		int parallelism = 10;

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		v1.setParallelism(parallelism);
		v2.setParallelism(parallelism);
		v3.setParallelism(parallelism);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", new JobVertex[] {v1, v2, v3});
		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
			jobGraph,
			new SimpleAckingTaskManagerGateway(),
			new NoRestartStrategy());

		final List<ExecutionVertex> executionVertices = new ArrayList<>();
		final List<ExecutionVertexID> vertices = new ArrayList<>();
		for (ExecutionJobVertex ejv : eg.getVerticesTopologically()) {
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				executionVertices.add(ev);
				vertices.add(ev.getExecutionVertexID());
			}
		}

		final Iterator<ExecutionJobVertex> ejvIterator = eg.getVerticesTopologically().iterator();
		final ExecutionJobVertex ejv1 = ejvIterator.next();
		final List<ExecutionVertexID> vertices1 = new ArrayList<>();
		for (ExecutionVertex ev : ejv1.getTaskVertices()) {
			vertices1.add(ev.getExecutionVertexID());
		}

		final ExecutionJobVertex ejv2 = ejvIterator.next();
		final List<ExecutionVertexID> vertices2 = new ArrayList<>();
		for (ExecutionVertex ev : ejv2.getTaskVertices()) {
			vertices2.add(ev.getExecutionVertexID());
		}

		final List<ExecutionVertexID> sourceVertices = new ArrayList<>();
		sourceVertices.addAll(vertices1);
		sourceVertices.addAll(vertices2);

		final ExecutionJobVertex ejv3 = ejvIterator.next();
		final List<ExecutionVertexID> vertices3 = new ArrayList<>();
		for (ExecutionVertex ev : ejv3.getTaskVertices()) {
			vertices3.add(ev.getExecutionVertexID());
		}

		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(eg, executionVertices);

		double pipelinedConsumableThreshold = 0.4;
		double blockingConsumableThreshold = 0.7;
		VertexInputTracker.VertexInputTrackerConfig inputTrackerConfig =
			new VertexInputTracker.VertexInputTrackerConfig(
				VertexInputTracker.InputDependencyConstraint.ALL,
				1,
				1
			);
		inputTrackerConfig.setInputDependencyConstraint(
			ejv3.getJobVertexId(),
			VertexInputTracker.InputDependencyConstraint.ANY
		);
		inputTrackerConfig.setInputConsumableThreshold(
			ejv3.getJobVertexId(),
			ejv1.getProducedDataSets()[0].getId(),
			pipelinedConsumableThreshold
		);
		inputTrackerConfig.setInputConsumableThreshold(
			ejv3.getJobVertexId(),
			ejv2.getProducedDataSets()[0].getId(),
			blockingConsumableThreshold
		);
		try {
			InstantiationUtil.writeObjectToConfig(inputTrackerConfig, jobGraph.getSchedulingConfiguration(),
				VertexInputTracker.VertexInputTrackerOptions.VERTEX_INPUT_TRACKER_CONFIG);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not serialize job vertex to stream node map", e);
		}

		final GraphManagerPlugin graphManagerPlugin = new StepwiseSchedulingPlugin();
		graphManagerPlugin.open(
			scheduler,
			jobGraph,
			new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()));

		graphManagerPlugin.onSchedulingStarted();
		assertTrue(compareVertices(scheduler.getScheduledVertices(), sourceVertices));
		scheduler.clearScheduledVertices();

		// Set the pipelined result consumable rate smaller than threshold
		ExecutionJobVertex ejv = ejv1;
		for (int i = 0; i < ejv.getParallelism() * pipelinedConsumableThreshold - 1; i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markDataProduced();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), Collections.EMPTY_SET));
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();

		// Set the pipelined result consumable rate larger than threshold
		ejv = ejv1;
		for (int i = 0; i < ejv.getParallelism() * pipelinedConsumableThreshold; i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markDataProduced();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices3));
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();

		// Set the blocking result consumable rate smaller than threshold
		ejv = ejv2;
		for (int i = 0; i < ejv.getParallelism() * blockingConsumableThreshold - 1; i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markFinished();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), Collections.EMPTY_SET));
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();

		// Set the blocking result consumable rate larger than threshold
		ejv = ejv2;
		for (int i = 0; i < ejv.getParallelism() * blockingConsumableThreshold; i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markFinished();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices3));
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();
	}

	/**
	 * Tests stepwise scheduling with configured input consumable status.
	 * Using percentile threshold and input constraints. Pointwise edges are introduced.
	 */
	@Test
	public void testStepwiseSchedulingConsideringInputStatusPointwisePercentile() throws Exception {

		int parallelism = 10;

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		v1.setParallelism(parallelism);
		v2.setParallelism(parallelism * 4);
		v3.setParallelism(parallelism * 2);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", new JobVertex[] {v1, v2, v3});
		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
			jobGraph,
			new SimpleAckingTaskManagerGateway(),
			new NoRestartStrategy());

		final List<ExecutionVertex> executionVertices = new ArrayList<>();
		final List<ExecutionVertexID> vertices = new ArrayList<>();
		for (ExecutionJobVertex ejv : eg.getVerticesTopologically()) {
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				executionVertices.add(ev);
				vertices.add(ev.getExecutionVertexID());
			}
		}

		final Iterator<ExecutionJobVertex> ejvIterator = eg.getVerticesTopologically().iterator();
		final ExecutionJobVertex ejv1 = ejvIterator.next();
		final List<ExecutionVertexID> vertices1 = new ArrayList<>();
		for (ExecutionVertex ev : ejv1.getTaskVertices()) {
			vertices1.add(ev.getExecutionVertexID());
		}

		final ExecutionJobVertex ejv2 = ejvIterator.next();
		final List<ExecutionVertexID> vertices2 = new ArrayList<>();
		for (ExecutionVertex ev : ejv2.getTaskVertices()) {
			vertices2.add(ev.getExecutionVertexID());
		}

		final List<ExecutionVertexID> sourceVertices = new ArrayList<>();
		sourceVertices.addAll(vertices1);
		sourceVertices.addAll(vertices2);

		final ExecutionJobVertex ejv3 = ejvIterator.next();
		final List<ExecutionVertexID> vertices3 = new ArrayList<>();
		for (ExecutionVertex ev : ejv3.getTaskVertices()) {
			vertices3.add(ev.getExecutionVertexID());
		}

		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(eg, executionVertices);

		double pipelinedConsumableThreshold = 0.7;
		double blockingConsumableThreshold = 0.7;
		VertexInputTracker.VertexInputTrackerConfig inputTrackerConfig =
			new VertexInputTracker.VertexInputTrackerConfig(
				VertexInputTracker.InputDependencyConstraint.ANY,
				pipelinedConsumableThreshold,
				blockingConsumableThreshold
			);
		try {
			InstantiationUtil.writeObjectToConfig(inputTrackerConfig, jobGraph.getSchedulingConfiguration(),
				VertexInputTracker.VertexInputTrackerOptions.VERTEX_INPUT_TRACKER_CONFIG);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not serialize job vertex to stream node map", e);
		}

		final GraphManagerPlugin graphManagerPlugin = new StepwiseSchedulingPlugin();
		graphManagerPlugin.open(
			scheduler,
			jobGraph,
			new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()));

		graphManagerPlugin.onSchedulingStarted();
		assertTrue(compareVertices(scheduler.getScheduledVertices(), sourceVertices));
		scheduler.clearScheduledVertices();

		// Set the pipelined result consumable rate smaller than threshold
		ExecutionJobVertex ejv = ejv1;
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(scheduler.getScheduledVertices().size() == 0);
		scheduler.clearScheduledVertices();

		// Set the pipelined result consumable rate larger than threshold
		ejv = ejv1;
		for (int i = 0; i < 1; i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markDataProduced();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(scheduler.getScheduledVertices().size() == 2);
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();

		// Set the other pipelined result consumable rate smaller than threshold
		ejv = ejv2;
		for (int i = 0; i < 1; i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markDataProduced();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(scheduler.getScheduledVertices().size() == 0);
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();

		// Set the other pipelined result consumable rate larger than threshold
		ejv = ejv2;
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markDataProduced();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv.getProducedDataSets()[0].getId(), 0));
		assertTrue(scheduler.getScheduledVertices().size() == 1);
		for (int i = 0; i < ejv.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(ejv.getJobVertexId())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.resetForNewExecution();
			}
		}
		scheduler.clearScheduledVertices();
	}
}
