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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.util.IOUtils;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of {@link StreamTaskInput} that reads data from the {@link SourceOperator}
 * and returns the {@link InputStatus} to indicate whether the source state is available,
 * unavailable or finished.
 */
@Internal
public final class StreamTaskSourceInput<T> implements StreamTaskInput<T> {

	private final SourceOperator<T, ?> operator;

	public StreamTaskSourceInput(SourceOperator<T, ?> operator) {
		this.operator = checkNotNull(operator);
	}

	@Override
	public InputStatus emitNext(DataOutput<T> output) throws Exception {
		return operator.emitNext(output);
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return operator.getAvailableFuture();
	}

	/**
	 * This method is invalid and never called by the one/source input processor.
	 */
	@Override
	public int getInputIndex() {
		return -1;
	}

	@Override
	public void close() {
		IOUtils.closeQuietly(operator::close);
	}

	@Override
	public CompletableFuture<Void> prepareSnapshot(
			ChannelStateWriter channelStateWriter,
			long checkpointId) {
		return CompletableFuture.completedFuture(null);
	}
}

