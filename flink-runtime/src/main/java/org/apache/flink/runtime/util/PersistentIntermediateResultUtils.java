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

package org.apache.flink.runtime.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to work with Persistent Intermediate Result.
 */
public class PersistentIntermediateResultUtils {

	/**
	 * Collect ShuffleDescriptor of an IntermediateResultPartition of BLOCKING_PERSISTENT type.
	 * throw exception if any error occurs.
	 */
	public static void collectShuffleDescriptor(
		ShuffleMaster<? extends ShuffleDescriptor> shuffleMaster,
		Map<AbstractID, Map<AbstractID, SerializedValue<Object>>> persistentIntermediateResultDescriptors,
		IntermediateResultPartition intermediateResultPartition) {

		IntermediateDataSetID dataSetID = intermediateResultPartition.getIntermediateResult().getId();

		Map<AbstractID, SerializedValue<Object>> resultPartitionMap = persistentIntermediateResultDescriptors.computeIfAbsent(
			new AbstractID(dataSetID), key -> new HashMap<>()
		);

		ShuffleDescriptor shuffleDescriptor = createShuffleDescriptor(shuffleMaster, intermediateResultPartition);

		Preconditions.checkNotNull(
			shuffleDescriptor,
			"Can't get ShuffleDescriptor of the ResultPartition: " + intermediateResultPartition.getPartitionId());

		SerializedValue<Object> serializedValue;
		try {
			serializedValue = new SerializedValue<>(shuffleDescriptor);
		} catch (IOException e) {
			throw new RuntimeException("Cound not serialize ShuffleDescriptor", e);
		}
		resultPartitionMap.put(new AbstractID(intermediateResultPartition.getPartitionId()), serializedValue);
	}

	@VisibleForTesting
	public static ShuffleDescriptor createShuffleDescriptor(ShuffleMaster<? extends ShuffleDescriptor> shuffleMaster,
													 IntermediateResultPartition intermediateResultPartition) {

		if (intermediateResultPartition.getResultType() != ResultPartitionType.BLOCKING_PERSISTENT) {
			return null;
		}

		// The taskManagerLocation should be ready already since the previous job is done.
		TaskManagerLocation taskManagerLocation = intermediateResultPartition
			.getProducer().getCurrentExecutionAttempt().getTaskManagerLocationFuture().getNow(null);

		Preconditions.checkNotNull(
			taskManagerLocation,
			"Can't get TaskManagerLocation of the ResultPartition: " + intermediateResultPartition.getPartitionId());

		ExecutionAttemptID producerId = intermediateResultPartition.getProducer().getCurrentExecutionAttempt().getAttemptId();

		return shuffleMaster.registerPartitionWithProducer(
			PartitionDescriptor.from(intermediateResultPartition),
			ProducerDescriptor.create(taskManagerLocation, producerId)
		).getNow(null);
	}
}
