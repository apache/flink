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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.DONE;
import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.EMPTY;
import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.PRODUCING;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link SchedulingResultPartition}.
 */
class DefaultSchedulingResultPartition implements SchedulingResultPartition {

	private final IntermediateResultPartitionID resultPartitionId;

	private final IntermediateDataSetID intermediateDataSetId;

	private final ResultPartitionType partitionType;

	private SchedulingExecutionVertex producer;

	private final List<SchedulingExecutionVertex> consumers;

	DefaultSchedulingResultPartition(
			IntermediateResultPartitionID partitionId,
			IntermediateDataSetID intermediateDataSetId,
			ResultPartitionType partitionType) {
		this.resultPartitionId = checkNotNull(partitionId);
		this.intermediateDataSetId = checkNotNull(intermediateDataSetId);
		this.partitionType = checkNotNull(partitionType);
		this.consumers = new ArrayList<>();
	}

	@Override
	public IntermediateResultPartitionID getId() {
		return resultPartitionId;
	}

	@Override
	public IntermediateDataSetID getResultId() {
		return intermediateDataSetId;
	}

	@Override
	public ResultPartitionType getPartitionType() {
		return partitionType;
	}

	@Override
	public ResultPartitionState getState() {
		switch (producer.getState()) {
			case RUNNING:
				return PRODUCING;
			case FINISHED:
				return DONE;
			default:
				return EMPTY;
		}
	}

	@Override
	public SchedulingExecutionVertex getProducer() {
		return producer;
	}

	@Override
	public Collection<SchedulingExecutionVertex> getConsumers() {
		return Collections.unmodifiableCollection(consumers);
	}

	void addConsumer(SchedulingExecutionVertex vertex) {
		consumers.add(checkNotNull(vertex));
	}

	void setProducer(SchedulingExecutionVertex vertex) {
		producer = checkNotNull(vertex);
	}
}
