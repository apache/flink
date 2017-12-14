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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.IOException;

/**
 * A buffer-oriented runtime result writer API for producing results.
 */
public interface ResultPartitionWriter {

	BufferProvider getBufferProvider();

	ResultPartitionID getPartitionId();

	int getNumberOfSubpartitions();

	int getNumTargetKeyGroups();

	/**
	 * Adds a buffer to the subpartition with the given index.
	 *
	 * <p>For PIPELINED {@link org.apache.flink.runtime.io.network.partition.ResultPartitionType}s,
	 * this will trigger the deployment of consuming tasks after the first buffer has been added.
	 */
	void writeBuffer(Buffer buffer, int subpartitionIndex) throws IOException;

	/**
	 * Writes the given buffer to all available target subpartitions.
	 *
	 * <p>The buffer is taken over and used for each of the channels.
	 * It will be recycled afterwards.
	 *
	 * @param buffer the buffer to write
	 */
	default void writeBufferToAllSubpartitions(final Buffer buffer) throws IOException {
		try {
			for (int subpartition = 0; subpartition < getNumberOfSubpartitions(); subpartition++) {
				// retain the buffer so that it can be recycled by each channel of targetPartition
				buffer.retain();
				writeBuffer(buffer, subpartition);
			}
		} finally {
			// we do not need to further retain the eventBuffer
			// (it will be recycled after the last channel stops using it)
			buffer.recycle();
		}
	}
}
