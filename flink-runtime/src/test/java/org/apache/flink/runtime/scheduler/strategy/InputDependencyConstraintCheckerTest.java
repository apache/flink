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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.api.common.InputDependencyConstraint.ALL;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link InputDependencyConstraintChecker}.
 */
public class InputDependencyConstraintCheckerTest extends TestLogger {

	@Test
	public void testCheckInputVertex() {
		final TestingSchedulingExecutionVertex vertex = TestingSchedulingExecutionVertex.newBuilder().build();
		final InputDependencyConstraintChecker inputChecker = createInputDependencyConstraintChecker(Collections.emptyList());

		assertTrue(inputChecker.check(vertex));
	}

	@Test
	public void testCheckCreatedPipelinedInput() {
		final List<TestingSchedulingResultPartition> partitions = addResultPartition()
			.withPartitionType(PIPELINED)
			.withPartitionState(ResultPartitionState.CREATED)
			.finish();
		final TestingSchedulingExecutionVertex vertex = TestingSchedulingExecutionVertex
				.newBuilder()
				.withConsumedPartitions(partitions)
				.build();

		final InputDependencyConstraintChecker inputChecker = createInputDependencyConstraintChecker(partitions);

		assertFalse(inputChecker.check(vertex));
	}

	@Test
	public void testCheckConsumablePipelinedInput() {
		final List<TestingSchedulingResultPartition> partitions = addResultPartition()
			.withPartitionType(PIPELINED)
			.withPartitionState(ResultPartitionState.CONSUMABLE)
			.finish();
		final TestingSchedulingExecutionVertex vertex = TestingSchedulingExecutionVertex
				.newBuilder()
				.withConsumedPartitions(partitions)
				.build();

		final InputDependencyConstraintChecker inputChecker = createInputDependencyConstraintChecker(partitions);

		assertTrue(inputChecker.check(vertex));
	}

	@Test
	public void testCheckDoneBlockingInput() {
		final List<TestingSchedulingResultPartition> partitions = addResultPartition()
			.withPartitionCntPerDataSet(2)
			.finish();
		final TestingSchedulingExecutionVertex vertex = TestingSchedulingExecutionVertex.newBuilder()
			.withConsumedPartitions(partitions)
			.build();

		final InputDependencyConstraintChecker inputChecker = createInputDependencyConstraintChecker(partitions);

		for (TestingSchedulingResultPartition srp : partitions) {
			inputChecker.markSchedulingResultPartitionFinished(srp);
		}

		assertTrue(inputChecker.check(vertex));
	}

	@Test
	public void testCheckPartialDoneBlockingInput() {
		final List<TestingSchedulingResultPartition> partitions = addResultPartition()
			.withPartitionCntPerDataSet(2)
			.finish();
		final TestingSchedulingExecutionVertex vertex = TestingSchedulingExecutionVertex
				.newBuilder()
				.withConsumedPartitions(partitions)
				.build();

		final InputDependencyConstraintChecker inputChecker = createInputDependencyConstraintChecker(partitions);

		inputChecker.markSchedulingResultPartitionFinished(partitions.get(0));

		assertFalse(inputChecker.check(vertex));
	}

	@Test
	public void testCheckResetBlockingInput() {
		final List<TestingSchedulingResultPartition> partitions = addResultPartition()
			.withPartitionCntPerDataSet(2)
			.finish();
		final TestingSchedulingExecutionVertex vertex = TestingSchedulingExecutionVertex
				.newBuilder()
				.withConsumedPartitions(partitions)
				.build();

		final InputDependencyConstraintChecker inputChecker = createInputDependencyConstraintChecker(partitions);

		for (TestingSchedulingResultPartition srp : partitions) {
			inputChecker.markSchedulingResultPartitionFinished(srp);
		}

		for (TestingSchedulingResultPartition srp : partitions) {
			inputChecker.resetSchedulingResultPartition(srp);
		}

		assertFalse(inputChecker.check(vertex));
	}

	@Test
	public void testCheckAnyBlockingInput() {
		final List<TestingSchedulingResultPartition> partitions = addResultPartition()
			.withDataSetCnt(2)
			.finish();
		final TestingSchedulingExecutionVertex vertex = TestingSchedulingExecutionVertex
				.newBuilder()
				.withConsumedPartitions(partitions)
				.build();

		final InputDependencyConstraintChecker inputChecker = createInputDependencyConstraintChecker(partitions);

		inputChecker.markSchedulingResultPartitionFinished(partitions.get(0));

		assertTrue(inputChecker.check(vertex));
	}

	@Test
	public void testCheckAllBlockingInput() {
		final List<TestingSchedulingResultPartition> partitions = addResultPartition()
			.withDataSetCnt(2)
			.finish();
		final TestingSchedulingExecutionVertex vertex = TestingSchedulingExecutionVertex.newBuilder()
			.withInputDependencyConstraint(ALL)
			.withConsumedPartitions(partitions)
			.build();

		final InputDependencyConstraintChecker inputChecker = createInputDependencyConstraintChecker(partitions);

		for (TestingSchedulingResultPartition srp : partitions) {
			inputChecker.markSchedulingResultPartitionFinished(srp);
		}

		assertTrue(inputChecker.check(vertex));
	}

	@Test
	public void testCheckAllPartialDatasetBlockingInput() {
		final List<TestingSchedulingResultPartition> partitions = addResultPartition()
			.withDataSetCnt(2)
			.finish();
		final TestingSchedulingExecutionVertex vertex = TestingSchedulingExecutionVertex
				.newBuilder()
				.withInputDependencyConstraint(ALL)
				.withConsumedPartitions(partitions)
				.build();

		final InputDependencyConstraintChecker inputChecker = createInputDependencyConstraintChecker(partitions);

		inputChecker.markSchedulingResultPartitionFinished(partitions.get(0));
		assertFalse(inputChecker.check(vertex));
	}

	@Test
	public void testCheckAllPartialPartitionBlockingInput() {
		final List<TestingSchedulingResultPartition> partitions = addResultPartition()
			.withDataSetCnt(2)
			.withPartitionCntPerDataSet(2)
			.finish();
		final TestingSchedulingExecutionVertex vertex = TestingSchedulingExecutionVertex
				.newBuilder()
				.withInputDependencyConstraint(ALL)
				.withConsumedPartitions(partitions)
				.build();

		final InputDependencyConstraintChecker inputChecker = createInputDependencyConstraintChecker(partitions);

		for (int idx = 0; idx < 3; idx++) {
			inputChecker.markSchedulingResultPartitionFinished(partitions.get(idx));
		}

		assertFalse(inputChecker.check(vertex));
	}

	private static TestingSchedulingResultPartitionBuilder addResultPartition() {
		return new TestingSchedulingResultPartitionBuilder();
	}

	private static InputDependencyConstraintChecker createInputDependencyConstraintChecker(
		List<TestingSchedulingResultPartition> partitions) {

		InputDependencyConstraintChecker inputChecker = new InputDependencyConstraintChecker();
		for (SchedulingResultPartition partition : partitions) {
			inputChecker.addSchedulingResultPartition(partition);
		}
		return inputChecker;
	}

	private static class TestingSchedulingResultPartitionBuilder {
		private int dataSetCnt = 1;
		private int partitionCntPerDataSet = 1;
		private ResultPartitionType partitionType = BLOCKING;
		private ResultPartitionState partitionState = ResultPartitionState.CONSUMABLE;

		TestingSchedulingResultPartitionBuilder withDataSetCnt(int dataSetCnt) {
			this.dataSetCnt = dataSetCnt;
			return this;
		}

		TestingSchedulingResultPartitionBuilder withPartitionCntPerDataSet(int partitionCnt) {
			this.partitionCntPerDataSet = partitionCnt;
			return this;
		}

		TestingSchedulingResultPartitionBuilder withPartitionType(ResultPartitionType type) {
			this.partitionType = type;
			return this;
		}

		TestingSchedulingResultPartitionBuilder withPartitionState(ResultPartitionState state) {
			this.partitionState = state;
			return this;
		}

		List<TestingSchedulingResultPartition> finish() {
			List<TestingSchedulingResultPartition> partitions = new ArrayList<>(dataSetCnt * partitionCntPerDataSet);
			for (int dataSetIdx = 0; dataSetIdx < dataSetCnt; dataSetIdx++) {
				IntermediateDataSetID dataSetId = new IntermediateDataSetID();
				for (int partitionIdx = 0; partitionIdx < partitionCntPerDataSet; partitionIdx++) {
					partitions.add(new TestingSchedulingResultPartition(dataSetId, partitionType, partitionState));
				}
			}

			return partitions;
		}
	}
}
