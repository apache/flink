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

import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A output data result of an individual task (one partition of an intermediate result),
 * produced and communicated in a batch manner: The result must be produced completely before
 * it can be consumed.
 *
 * <p>In this particular implementation, the batch result is written to (and read from) one file
 * per sub-partition. This implementation hence requires at least as many files (file handles) and
 * memory buffers as the parallelism of the target task that the data is shuffled to.
 */
public class BoundedBlockingResultPartition extends BufferWritingResultPartition {

	public BoundedBlockingResultPartition(
			String owningTaskName,
			int partitionIndex,
			ResultPartitionID partitionId,
			ResultPartitionType partitionType,
			ResultSubpartition[] subpartitions,
			int numTargetKeyGroups,
			ResultPartitionManager partitionManager,
			@Nullable BufferCompressor bufferCompressor,
			SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

		super(
			owningTaskName,
			partitionIndex,
			partitionId,
			checkResultPartitionType(partitionType),
			subpartitions,
			numTargetKeyGroups,
			partitionManager,
			bufferCompressor,
			bufferPoolFactory);
	}

	@Override
	public void flush(int targetSubpartition) {
		flushSubpartition(targetSubpartition, true);
	}

	@Override
	public void flushAll() {
		flushAllSubpartitions(true);
	}

	private static ResultPartitionType checkResultPartitionType(ResultPartitionType type) {
		checkArgument(type == ResultPartitionType.BLOCKING || type == ResultPartitionType.BLOCKING_PERSISTENT);
		return type;
	}
}
