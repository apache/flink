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

import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
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
	 * Adds the bufferConsumer to the subpartition with the given index.
	 *
	 * <p>For PIPELINED {@link org.apache.flink.runtime.io.network.partition.ResultPartitionType}s,
	 * this will trigger the deployment of consuming tasks after the first buffer has been added.
	 *
	 * <p>This method takes the ownership of the passed {@code bufferConsumer} and thus is responsible for releasing
	 * it's resources.
	 *
	 * <p>To avoid problems with data re-ordering, before adding new {@link BufferConsumer} the previously added one
	 * the given {@code subpartitionIndex} must be marked as {@link BufferConsumer#isFinished()}.
	 */
	void addBufferConsumer(BufferConsumer bufferConsumer, int subpartitionIndex) throws IOException;

	/**
	 * Manually trigger consumption from enqueued {@link BufferConsumer BufferConsumers} in all subpartitions.
	 */
	void flushAll();

	/**
	 * Manually trigger consumption from enqueued {@link BufferConsumer BufferConsumers} in one specified subpartition.
	 */
	void flush(int subpartitionIndex);
}
