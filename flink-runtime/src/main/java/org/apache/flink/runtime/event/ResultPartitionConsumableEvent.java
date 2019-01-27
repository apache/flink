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

package org.apache.flink.runtime.event;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

/**
 * Event for consumable result partition.
 */
public class ResultPartitionConsumableEvent extends ExecutionEvent {

	private final IntermediateDataSetID resultID;

	private final int partitionNumber;

	public ResultPartitionConsumableEvent(
		final IntermediateDataSetID resultID,
		final int partitionNumber) {

		this.resultID = resultID;
		this.partitionNumber = partitionNumber;
	}

	public IntermediateDataSetID getResultID() {
		return resultID;
	}

	public int getPartitionNumber() {
		return partitionNumber;
	}
}
