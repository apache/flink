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

package org.apache.flink.runtime.executiongraph.failover.adapter;

import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverEdge;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link FailoverEdge}.
 */
class DefaultFailoverEdge implements FailoverEdge {

	private final IntermediateResultPartitionID resultPartitionId;

	private final ResultPartitionType partitionType;

	private final DefaultFailoverVertex sourceVertex;

	private final DefaultFailoverVertex targetVertex;

	DefaultFailoverEdge(
		IntermediateResultPartitionID partitionId,
		ResultPartitionType partitionType,
		DefaultFailoverVertex sourceVertex,
		DefaultFailoverVertex targetVertex) {

		this.resultPartitionId = checkNotNull(partitionId);
		this.partitionType = checkNotNull(partitionType);
		this.sourceVertex = checkNotNull(sourceVertex);
		this.targetVertex = checkNotNull(targetVertex);
	}

	@Override
	public IntermediateResultPartitionID getResultPartitionID() {
		return resultPartitionId;
	}

	@Override
	public ResultPartitionType getResultPartitionType() {
		return partitionType;
	}

	@Override
	public FailoverVertex getSourceVertex() {
		return sourceVertex;
	}

	@Override
	public FailoverVertex getTargetVertex() {
		return targetVertex;
	}
}
