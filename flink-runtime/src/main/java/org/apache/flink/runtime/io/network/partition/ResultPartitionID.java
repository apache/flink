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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.Serializable;

/**
 * Runtime identifier of a produced {@link IntermediateResultPartition}.
 *
 * <p> In failure cases the {@link IntermediateResultPartitionID} is not enough to uniquely
 * identify a result partition. It needs to be associated with the producing task as well to ensure
 * correct tracking of failed/restarted tasks.
 */
public final class ResultPartitionID implements Serializable {

	private static final long serialVersionUID = -902516386203787826L;

	private final IntermediateResultPartitionID partitionId;

	private final ExecutionAttemptID producerId;

	public ResultPartitionID() {
		this(new IntermediateResultPartitionID(), new ExecutionAttemptID());
	}

	public ResultPartitionID(IntermediateResultPartitionID partitionId, ExecutionAttemptID producerId) {
		this.partitionId = partitionId;
		this.producerId = producerId;
	}

	public IntermediateResultPartitionID getPartitionId() {
		return partitionId;
	}

	public ExecutionAttemptID getProducerId() {
		return producerId;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj.getClass() == ResultPartitionID.class) {
			ResultPartitionID o = (ResultPartitionID) obj;

			return o.getPartitionId().equals(partitionId) && o.getProducerId().equals(producerId);
		}

		return false;
	}

	@Override
	public int hashCode() {
		return partitionId.hashCode() ^ producerId.hashCode();
	}

	@Override
	public String toString() {
		return partitionId.toString() + "@" + producerId.toString();
	}
}
