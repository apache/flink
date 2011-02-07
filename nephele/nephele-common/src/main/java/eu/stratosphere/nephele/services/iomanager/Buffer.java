/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.services.iomanager;

import java.io.DataInput;
import java.io.IOException;
import java.nio.channels.FileChannel;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemoryBacked;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.UnboundMemoryBackedException;

/**
 * An abstract base class for IO buffers.
 * 
 * @author Alexander Alexandrov
 * @param <T>
 *        the type of the underlying memory segment.
 */
abstract public class Buffer extends MemoryBacked {
	/**
	 * Position in the underlying memory.
	 */
	protected int position;

	/**
	 * Limit of the underlying memory.
	 */
	protected int limit;

	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	/**
	 * Creates an unbound buffer. {@code position} and {@code limit} are set to
	 * 0.
	 */
	public Buffer() {
		super();
		position = 0;
		limit = 0;
	}

	/**
	 * Returns the number of remaining bytes (computed as the difference between
	 * the {@code limit} and the {@code position}).
	 */
	protected final int getRemainingBytes() {
		return limit - position;
	}

	/**
	 * Returns the current position of this buffer.
	 * 
	 * @return
	 */
	public final int getPosition() {
		return position;
	}

	/**
	 * Binds the IO buffer to a {@link MemorySegment} and resets is with the
	 * limit set to the memory segment's {@code size}. If the buffer is already
	 * bound, the operation has no effect.
	 */
	@Override
	public final boolean bind(MemorySegment memory) {
		if (super.bind(memory)) {
			reset(memory.size());
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Resets the {@link DataInputView} and {@link DataOutputView} of the
	 * memory, sets the position to zero and the limit to the provided {@code l} value.
	 * 
	 * @param l
	 *        the new limit to be set
	 */
	public final void reset(int l) {
		memory.inputView.reset();
		memory.outputView.reset();
		position = 0;
		limit = l;
	}

	/**
	 * Resets the {@link DataInputView} and {@link DataOutputView} of the
	 * memory, sets the position to zero but leaves the limit unchanged.
	 */
	public final void reset() {
		memory.inputView.reset();
		memory.outputView.reset();
		position = 0;
	}

	/**
	 * An input buffer for IOReadableWritable objects.
	 * 
	 * @author Alexander Alexandrov
	 */
	public static class Input extends Buffer {
		public Input() {
			super();
		}

		/**
		 * Reads an {@code IOReadableWritable} from the underlying memory
		 * segment into the provided {@link IOReadableWritable} object by
		 * calling it's {@link IOReadableWritable#read(DataInput)} operation
		 * with the backing memory's {@link DataInputView}.
		 * 
		 * @param object
		 *        to read in from the buffer
		 * @return a boolean value indicating whether the read was successfull
		 * @throws UnboundMemoryBackedException
		 */
		public boolean read(IOReadableWritable object) {
			if (!isBound()) {
				throw new UnboundMemoryBackedException();
			}

			try {
				if (position >= limit) {
					return false;
				}

				memory.inputView.skip(4); // skip serialized length
				object.read(memory.inputView); // read object
				position = memory.inputView.getPosition(); // update current read position
				return position <= limit; // ok if read limit was not exceeded while reading
			} catch (IOException e) {
				return false;
			}
		}

		/**
		 * Reads the buffer from a Java NIO {@link FileChannel}. Upon reading,
		 * the serialized {@link IOReadableWritable} lengths are traversed to
		 * compute the buffer limit and eventually the channel position and the
		 * buffer limit are adapted accordingly.
		 * 
		 * @param channel
		 *        the NIO file channel to read from
		 * @throws IOException
		 *         thrown by the {@link FileChannel#read(java.nio.ByteBuffer)} method.
		 * @throws UnboundMemoryBackedException
		 */
		public void readFromChannel(FileChannel channel) throws IOException {
			if (!isBound()) {
				throw new UnboundMemoryBackedException();
			}

			// read either the full buffer size or the remaining bytes from the channel
			int limit = (int) Math.min(memory.size(), channel.size() - channel.position());
			channel.read(memory.wrap(0, limit));

			// find the end of the last fully contained object in the buffer
			int readableBytes = 0, currentObjectLength = 0;
			while (readableBytes + currentObjectLength <= limit) {
				// accumulate next serialized object length
				readableBytes += currentObjectLength;

				// if next object length goes beyond the limit, break
				if (readableBytes + 4 >= limit) {
					break;
				}

				// else get next object length
				currentObjectLength = 4 + memory.randomAccessView.getInt(readableBytes);
			}

			// reset buffer with the limit set to the number of readable bytes
			reset(readableBytes);
			// adapt channel position for next buffer
			channel.position(channel.position() - (limit - readableBytes));
		}
	}

	/**
	 * An output buffer for {@code IOReadableWritable} objects.
	 * 
	 * @author Alexander Alexandrov
	 */
	public static class Output extends Buffer {

		public Output() {
			super();
		}

		/**
		 * Tries to write the provided {@link IOReadableWritable} to the
		 * underlying memory segment by calling the object's {@link IOReadableWritable#read(DataInput)} operation with
		 * the backing
		 * memory {@link DataInputView}.
		 * 
		 * @param object
		 *        to write out to the buffer
		 * @return a boolean value indicating whether the write was successful
		 * @throws UnboundMemoryBackedException
		 */
		public boolean write(IOReadableWritable object) {
			if (!isBound()) {
				throw new UnboundMemoryBackedException();
			}

			try {
				memory.outputView.skip(4); // reserve bytes for length
				object.write(memory.outputView); // serialize object
				memory.randomAccessView.putInt(position, memory.outputView.getPosition() - position - 4); // serialize
				// object
				// length
				position = memory.outputView.getPosition(); // update current write position
				return true;
			} catch (IOException e) {
				return false;
			}
		}

		/**
		 * Writes all written objects to the provided Java NIO {@link FileChannel}.
		 * 
		 * @param channel
		 * @throws IOException
		 */
		public void writeToChannel(FileChannel channel) throws IOException {
			if (!isBound()) {
				new UnboundMemoryBackedException();
			}

			channel.write(memory.wrap(0, position));
			reset(memory.size());
		}
	}

	// -------------------------------------------------------------------------
	// Typing
	// -------------------------------------------------------------------------

	/**
	 * Enumeration class for the different buffer types.
	 * 
	 * @author Alexander Alexandrov
	 * @param <T>
	 *        the buffer class identified by the {@code Type} instance
	 */
	public static final class Type<T extends Buffer> {
		public static final Type<Input> INPUT = new Type<Input>(Input.class);

		public static final Type<Output> OUTPUT = new Type<Output>(Output.class);

		public final Class<T> clazz;

		private Type(Class<T> clazz) {
			this.clazz = clazz;
		}
	}
}
