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

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.io.Serializable;

/**
 * This class represents current status of a result partition.
 */
public class ResultPartitionStatus implements Serializable {

	private final IntermediateDataSetID resultID;

	private final int partitionNumber;

	private final boolean isConsumable;

	public ResultPartitionStatus(
		final IntermediateDataSetID resultID,
		final int partitionNumber,
		final boolean isConsumable) {

		this.resultID = resultID;
		this.partitionNumber = partitionNumber;
		this.isConsumable = isConsumable;

	}

	/**
	 * Get the id of the result.
	 *
	 * @return id of the result
	 */
	IntermediateDataSetID getResultID() {
		return resultID;
	}

	/**
	 * Get the partition number.
	 *
	 * @return partition number
	 */
	int getPartitionNumber() {
		return partitionNumber;
	}

	/**
	 * Get whether the result partition is consumable.
	 *
	 * @return whether the result partition is consumable
	 */
	boolean isConsumable() {
		return isConsumable;
	}
}
