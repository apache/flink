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

import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.Collection;

/**
 * Representation of {@link IntermediateResultPartition}.
 */
public interface SchedulingResultPartition {

	/**
	 * Gets id of the result partition.
	 *
	 * @return id of the result partition
	 */
	IntermediateResultPartitionID getId();

	/**
	 * Gets id of the intermediate result.
	 *
	 * @return id of the intermediate result
	 */
	IntermediateDataSetID getResultId();

	/**
	 * Gets the {@link ResultPartitionType}.
	 *
	 * @return result partition type
	 */
	ResultPartitionType getPartitionType();

	/**
	 * Gets the {@link ResultPartitionState}.
	 *
	 * @return result partition state
	 */
	ResultPartitionState getState();

	/**
	 * Gets the producer of this result partition.
	 *
	 * @return producer vertex of this result partition
	 */
	SchedulingExecutionVertex getProducer();

	/**
	 * Gets the consumers of this result partition.
	 *
	 * @return Collection of consumer vertices of this result partition
	 */
	Collection<SchedulingExecutionVertex> getConsumers();

	/**
	 * State of the result partition.
	 */
	enum ResultPartitionState {
		/**
		 * Producer is not yet running or in abnormal state.
		 */
		EMPTY,

		/**
		 * Producer is running.
		 */
		PRODUCING,

		/**
		 * Producer has terminated.
		 */
		DONE,

		/**
		 * Partition has been released.
		 */
		RELEASED
	}
}
