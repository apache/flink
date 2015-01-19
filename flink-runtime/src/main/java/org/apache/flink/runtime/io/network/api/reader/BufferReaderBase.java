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

package org.apache.flink.runtime.io.network.api.reader;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;

/**
 * A buffer-oriented runtime result reader.
 * <p>
 * {@link BufferReaderBase} is the runtime API for consuming results. Events
 * are handled by the reader and users can query for buffers with
 * {@link #getNextBufferBlocking()} or {@link #getNextBuffer(Buffer)}.
 * <p>
 * <strong>Important</strong>: If {@link #getNextBufferBlocking()} is used, it is
 * necessary to release the returned buffers with {@link Buffer#recycle()}
 * after they are consumed.
 */
public interface BufferReaderBase extends ReaderBase {

	/**
	 * Returns the next queued {@link Buffer} from one of the {@link RemoteInputChannel}
	 * instances attached to this reader. The are no ordering guarantees with
	 * respect to which channel is queried for data.
	 * <p>
	 * <strong>Important</strong>: it is necessary to release buffers, which
	 * are returned by the reader via {@link Buffer#recycle()}, because they
	 * are a pooled resource. If not recycled, the network stack will run out
	 * of buffers and deadlock.
	 *
	 * @see #getChannelIndexOfLastBuffer()
	 */
	Buffer getNextBufferBlocking() throws IOException, InterruptedException;

	/**
	 * {@link #getNextBufferBlocking()} requires the user to quickly recycle the
	 * returned buffer. For a fully buffer-oriented runtime, we need to
	 * support a variant of this method, which allows buffers to be exchanged
	 * in order to save unnecessary memory copies between buffer pools.
	 * <p>
	 * Currently this is not a problem, because the only "users" of the buffer-
	 * oriented API are the record-oriented readers, which immediately
	 * deserialize the buffer and recycle it.
	 */
	Buffer getNextBuffer(Buffer exchangeBuffer) throws IOException, InterruptedException;

	/**
	 * Returns a channel index for the last {@link Buffer} instance returned by
	 * {@link #getNextBufferBlocking()} or {@link #getNextBuffer(Buffer)}.
	 * <p>
	 * The returned index is guaranteed to be the same for all buffers read by
	 * the same {@link RemoteInputChannel} instance. This is useful when data spans
	 * multiple buffers returned by this reader.
	 * <p>
	 * Initially returns <code>-1</code> and if multiple readers are unioned,
	 * the local channel indexes are mapped to the sequence from 0 to n-1.
	 */
	int getChannelIndexOfLastBuffer();

	/**
	 * Returns the total number of {@link InputChannel} instances, from which this
	 * reader gets its data.
	 */
	int getNumberOfInputChannels();

	boolean isTaskEvent();

	void subscribeToReader(EventListener<BufferReaderBase> listener);

	void requestPartitionsOnce() throws IOException;

}
