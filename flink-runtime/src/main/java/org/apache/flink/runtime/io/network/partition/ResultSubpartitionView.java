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

import javax.annotation.Nullable;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A view to consume a {@link ResultSubpartition} instance.
 */
public abstract class ResultSubpartitionView {

	/** The parent subpartition this view belongs to. */
	private final ResultSubpartition parent;

	public ResultSubpartitionView(ResultSubpartition parent) {
		this.parent = checkNotNull(parent);
	}

	/**
	 * Returns the number of non-event buffers in this result subpartition.
	 */
	public int getBuffersInBacklog() {
		return parent.getBuffersInBacklog();
	}

	/**
	 * Returns the next {@link Buffer} instance of this queue iterator.
	 *
	 * <p>If there is currently no instance available, it will return <code>null</code>.
	 * This might happen for example when a pipelined queue producer is slower
	 * than the consumer or a spilled queue needs to read in more data.
	 *
	 * <p><strong>Important</strong>: The consumer has to make sure that each
	 * buffer instance will eventually be recycled with {@link Buffer#recycle()}
	 * after it has been consumed.
	 */
	@Nullable
	public abstract Buffer getNextBuffer() throws IOException, InterruptedException;

	public abstract void notifyBuffersAvailable(long buffers) throws IOException;

	public abstract void releaseAllResources() throws IOException;

	public abstract void notifySubpartitionConsumed() throws IOException;

	public abstract boolean isReleased();

	public abstract Throwable getFailureCause();
}
