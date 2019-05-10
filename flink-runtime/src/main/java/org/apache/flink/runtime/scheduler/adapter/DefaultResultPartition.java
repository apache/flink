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

import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
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
import static org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition.ResultPartitionState.RELEASED;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link SchedulingResultPartition}.
 */
public class DefaultResultPartition implements SchedulingResultPartition {

	private final IntermediateResultPartition resultPartition;

	private final SchedulingExecutionVertex producer;

	private final List<SchedulingExecutionVertex> consumers;

	DefaultResultPartition(IntermediateResultPartition partition, SchedulingExecutionVertex producer) {
		this.resultPartition = checkNotNull(partition);
		this.producer = checkNotNull(producer);
		this.consumers = new ArrayList<>();
	}

	@Override
	public IntermediateResultPartitionID getId() {
		return resultPartition.getPartitionId();
	}

	@Override
	public IntermediateDataSetID getResultId() {
		return resultPartition.getIntermediateResult().getId();
	}

	@Override
	public ResultPartitionType getPartitionType() {
		return resultPartition.getResultType();
	}

	@Override
	public ResultPartitionState getState() {
		switch (producer.getState()) {
			case CREATED:
			case SCHEDULED:
			case DEPLOYING:
				return EMPTY;
			case RUNNING:
				return PRODUCING;
			case FINISHED:
				return DONE;
			default:
				return RELEASED;
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
}
