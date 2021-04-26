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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

import java.nio.ByteBuffer;

/**
 * Wrapper for pooled {@link MemorySegment} instances with reference counting.
 *
 * <p>This is similar to Netty's <tt>ByteBuf</tt> with some extensions and restricted to the methods
 * our use cases outside Netty handling use. In particular, we use two different indexes for read
 * and write operations, i.e. the <tt>reader</tt> and <tt>writer</tt> index (size of written data),
 * which specify three regions inside the memory segment:
 * <pre>
 *     +-------------------+----------------+----------------+
 *     | discardable bytes | readable bytes | writable bytes |
 *     +-------------------+----------------+----------------+
 *     |                   |                |                |
 *     0      <=      readerIndex  <=  writerIndex   <=  max capacity
 * </pre>
 *
 * <p>Our non-Netty usages of this <tt>Buffer</tt> class either rely on the underlying {@link
 * #getMemorySegment()} directly, or on {@link ByteBuffer} wrappers of this buffer which do not
 * modify either index, so the indices need to be updated manually via {@link #setReaderIndex(int)}
 * and {@link #setSize(int)}.
 */
public interface Buffer {

	/**
	 * Returns whether this buffer represents a buffer or an event.
	 *
	 * @return <tt>true</tt> if this is a real buffer, <tt>false</tt> if this is an event
	 */
	boolean isBuffer();

	/**
	 * Returns the underlying memory segment. This method is dangerous since it ignores read only protections and omits
	 * slices. Use it only along the {@link #getMemorySegmentOffset()}.
	 *
	 * <p>This method will be removed in the future. For writing use {@link BufferBuilder}.
	 *
	 * @return the memory segment backing this buffer
	 */
	@Deprecated
	MemorySegment getMemorySegment();

	/**
	 * This method will be removed in the future. For writing use {@link BufferBuilder}.
	 *
	 * @return the offset where this (potential slice) {@link Buffer}'s data start in the underlying memory segment.
	 */
	@Deprecated
	int getMemorySegmentOffset();

	/**
	 * Gets the buffer's recycler.
	 *
	 * @return buffer recycler
	 */
	BufferRecycler getRecycler();

	/**
	 * Releases this buffer once, i.e. reduces the reference count and recycles the buffer if the
	 * reference count reaches <tt>0</tt>.
	 *
	 * @see #retainBuffer()
	 */
	void recycleBuffer();

	/**
	 * Returns whether this buffer has been recycled or not.
	 *
	 * @return <tt>true</tt> if already recycled, <tt>false</tt> otherwise
	 */
	boolean isRecycled();

	/**
	 * Retains this buffer for further use, increasing the reference counter by <tt>1</tt>.
	 *
	 * @return <tt>this</tt> instance (for chained calls)
	 *
	 * @see #recycleBuffer()
	 */
	Buffer retainBuffer();

	/**
	 * Returns a read-only slice of this buffer's readable bytes, i.e. between
	 * {@link #getReaderIndex()} and {@link #getSize()}.
	 *
	 * <p>Reader and writer indices as well as markers are not shared. Reference counters are
	 * shared but the slice is not {@link #retainBuffer() retained} automatically.
	 *
	 * @return a read-only sliced buffer
	 */
	Buffer readOnlySlice();

	/**
	 * Returns a read-only slice of this buffer.
	 *
	 * <p>Reader and writer indices as well as markers are not shared. Reference counters are
	 * shared but the slice is not {@link #retainBuffer() retained} automatically.
	 *
	 * @param index the index to start from
	 * @param length the length of the slice
	 *
	 * @return a read-only sliced buffer
	 */
	Buffer readOnlySlice(int index, int length);

	/**
	 * Returns the maximum size of the buffer, i.e. the capacity of the underlying {@link MemorySegment}.
	 *
	 * @return size of the buffer
	 */
	int getMaxCapacity();

	/**
	 * Returns the <tt>reader index</tt> of this buffer.
	 *
	 * <p>This is where readable (unconsumed) bytes start in the backing memory segment.
	 *
	 * @return reader index (from 0 (inclusive) to the size of the backing {@link MemorySegment}
	 * (inclusive))
	 */
	int getReaderIndex();

	/**
	 * Sets the <tt>reader index</tt> of this buffer.
	 *
	 * @throws IndexOutOfBoundsException
	 * 		if the index is less than <tt>0</tt> or greater than {@link #getSize()}
	 */
	void setReaderIndex(int readerIndex) throws IndexOutOfBoundsException;

	/**
	 * Returns the size of the written data, i.e. the <tt>writer index</tt>, of this buffer.
	 *
	 * <p>This is where writable bytes start in the backing memory segment.
	 *
	 * @return writer index (from 0 (inclusive) to the size of the backing {@link MemorySegment}
	 * (inclusive))
	 */
	int getSize();

	/**
	 * Sets the size of the written data, i.e. the <tt>writer index</tt>, of this buffer.
	 *
	 * @throws IndexOutOfBoundsException
	 * 		if the index is less than {@link #getReaderIndex()} or greater than {@link #getMaxCapacity()}
	 */
	void setSize(int writerIndex);

	/**
	 * Returns the number of readable bytes (same as <tt>{@link #getSize()} -
	 * {@link #getReaderIndex()}</tt>).
	 */
	int readableBytes();

	/**
	 * Gets a new {@link ByteBuffer} instance wrapping this buffer's readable bytes, i.e. between
	 * {@link #getReaderIndex()} and {@link #getSize()}.
	 *
	 * <p>Please note that neither index is updated by the returned buffer.
	 *
	 * @return byte buffer sharing the contents of the underlying memory segment
	 */
	ByteBuffer getNioBufferReadable();

	/**
	 * Gets a new {@link ByteBuffer} instance wrapping this buffer's bytes.
	 *
	 * <p>Please note that neither <tt>read</tt> nor <tt>write</tt> index are updated by the
	 * returned buffer.
	 *
	 * @return byte buffer sharing the contents of the underlying memory segment
	 *
	 * @throws IndexOutOfBoundsException
	 * 		if the indexes are not without the buffer's bounds
	 * @see #getNioBufferReadable()
	 */
	ByteBuffer getNioBuffer(int index, int length) throws IndexOutOfBoundsException;

	/**
	 * Sets the buffer allocator for use in netty.
	 *
	 * @param allocator netty buffer allocator
	 */
	void setAllocator(ByteBufAllocator allocator);

	/**
	 * @return self as ByteBuf implementation.
	 */
	ByteBuf asByteBuf();

	/**
	 * @return whether the buffer is compressed or not.
	 */
	boolean isCompressed();

	/**
	 * Tags the buffer as compressed or uncompressed.
	 */
	void setCompressed(boolean isCompressed);

	/**
	 * Gets the type of data this buffer represents.
	 */
	DataType getDataType();

	/**
	 * Sets the type of data this buffer represents.
	 */
	void setDataType(DataType dataType);

	/**
	 * Used to identify the type of data contained in the {@link Buffer} so that we can get
	 * the information without deserializing the serialized data.
	 *
	 * <p>Notes: Currently, one byte is used to serialize the ordinal of {@link DataType} in
	 * {@link org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse}, so the
	 * maximum number of supported data types is 128.
	 */
	enum DataType {
		/**
		 * {@link #NONE} indicates that there is no buffer.
		 */
		NONE(false, false, false, false),

		/**
		 * {@link #DATA_BUFFER} indicates that this buffer represents a non-event data buffer.
		 */
		DATA_BUFFER(true, false, false, false),

		/**
		 * {@link #EVENT_BUFFER} indicates that this buffer represents serialized data of an event.
		 * Note that this type can be further divided into more fine-grained event types
		 * like {@link #ALIGNED_CHECKPOINT_BARRIER} and etc.
		 */
		EVENT_BUFFER(false, true, false, false),

		/**
		 * Same as EVENT_BUFFER, but the event has been prioritized (e.g. it skipped buffers).
		 */
		PRIORITIZED_EVENT_BUFFER(false, true, false, true),

		/**
		 * {@link #ALIGNED_CHECKPOINT_BARRIER} indicates that this buffer represents a
		 * serialized checkpoint barrier of aligned exactly-once checkpoint mode.
		 */
		ALIGNED_CHECKPOINT_BARRIER(false, true, true, false);

		private final boolean isBuffer;
		private final boolean isEvent;
		private final boolean isBlockingUpstream;
		private final boolean hasPriority;

		DataType(boolean isBuffer, boolean isEvent, boolean isBlockingUpstream, boolean hasPriority) {
			this.isBuffer = isBuffer;
			this.isEvent = isEvent;
			this.isBlockingUpstream = isBlockingUpstream;
			this.hasPriority = hasPriority;
		}

		public boolean isBuffer() {
			return isBuffer;
		}

		public boolean isEvent() {
			return isEvent;
		}

		public boolean hasPriority() {
			return hasPriority;
		}

		public boolean isBlockingUpstream() {
			return isBlockingUpstream;
		}

		public static DataType getDataType(AbstractEvent event, boolean hasPriority) {
			if (hasPriority) {
				return PRIORITIZED_EVENT_BUFFER;
			}
			return event instanceof CheckpointBarrier && ((CheckpointBarrier) event).getCheckpointOptions().needsAlignment() ?
					ALIGNED_CHECKPOINT_BARRIER :
					EVENT_BUFFER;
		}
	}
}
