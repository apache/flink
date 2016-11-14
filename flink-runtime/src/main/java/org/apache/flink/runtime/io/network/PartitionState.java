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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Contains information about the state of a result partition.
 */
public class PartitionState implements Serializable {

	private static final long serialVersionUID = -4693651272083825031L;

	private final IntermediateDataSetID intermediateDataSetID;
	private final IntermediateResultPartitionID intermediateResultPartitionID;
	private final ExecutionState executionState;

	public PartitionState(
		IntermediateDataSetID intermediateDataSetID,
		IntermediateResultPartitionID intermediateResultPartitionID,
		@Nullable ExecutionState executionState) {

		this.intermediateDataSetID = Preconditions.checkNotNull(intermediateDataSetID);
		this.intermediateResultPartitionID = Preconditions.checkNotNull(intermediateResultPartitionID);
		this.executionState = executionState;
	}

	public IntermediateDataSetID getIntermediateDataSetID() {
		return intermediateDataSetID;
	}

	public IntermediateResultPartitionID getIntermediateResultPartitionID() {
		return intermediateResultPartitionID;
	}

	/**
	 * Returns the execution state of the partition producer or <code>null</code> if it is not available.
	 */
	public ExecutionState getExecutionState() {
		return executionState;
	}
}
