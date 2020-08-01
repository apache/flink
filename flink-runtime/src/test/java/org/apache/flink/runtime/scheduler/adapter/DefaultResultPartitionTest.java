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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.function.Supplier;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link DefaultResultPartition}.
 */
public class DefaultResultPartitionTest extends TestLogger {

	private static final TestResultPartitionStateSupplier resultPartitionState = new TestResultPartitionStateSupplier();

	private final IntermediateResultPartitionID resultPartitionId = new IntermediateResultPartitionID();
	private final IntermediateDataSetID intermediateResultId = new IntermediateDataSetID();

	private DefaultResultPartition resultPartition;

	@Before
	public void setUp() {
		resultPartition = new DefaultResultPartition(
			resultPartitionId,
			intermediateResultId,
			BLOCKING,
			resultPartitionState);
	}

	@Test
	public void testGetPartitionState() {
		for (ResultPartitionState state : ResultPartitionState.values()) {
			resultPartitionState.setResultPartitionState(state);
			assertEquals(state, resultPartition.getState());
		}
	}

	/**
	 * A test {@link ResultPartitionState} supplier.
	 */
	private static class TestResultPartitionStateSupplier implements Supplier<ResultPartitionState> {

		private ResultPartitionState resultPartitionState;

		void setResultPartitionState(ResultPartitionState state) {
			resultPartitionState = state;
		}

		@Override
		public ResultPartitionState get() {
			return resultPartitionState;
		}
	}
}
