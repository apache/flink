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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A view to consume a {@link ResultSubpartition} instance.
 */
public interface ResultSubpartitionView {

	/**
	 * Returns the next {@link Buffer} instance of this queue iterator.
	 *
	 * <p>If there is currently no instance available, it will return <code>null</code>.
	 * This might happen for example when a pipelined queue producer is slower
	 * than the consumer or a spilled queue needs to read in more data.
	 *
	 * <p><strong>Important</strong>: The consumer has to make sure that each
	 * buffer instance will eventually be recycled with {@link Buffer#recycleBuffer()}
	 * after it has been consumed.
	 */
	@Nullable
	BufferAndBacklog getNextBuffer() throws IOException, InterruptedException;

	void notifyDataAvailable();

	void releaseAllResources() throws IOException;

	void notifySubpartitionConsumed() throws IOException;

	boolean isReleased();

	Throwable getFailureCause();

	/**
	 * Returns whether the next buffer is an event or not.
	 */
	boolean nextBufferIsEvent();

	boolean isAvailable();
}
