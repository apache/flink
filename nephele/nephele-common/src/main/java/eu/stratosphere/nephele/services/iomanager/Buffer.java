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
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.UnboundMemoryBackedException;

/**
 * An abstract base class for IO buffers.
 * 
 * @author Alexander Alexandrov
 * @author Stephan Ewen
 */
public abstract class Buffer {

	/**
	 * The memory segment that backs this buffer.
	 */
	protected MemorySegment memory;

	// -------------------------------------------------------------------------
	//                     Constructors / Destructors
	// -------------------------------------------------------------------------

	/**
	 * Creates an unbound buffer. {@code position} and {@code limit} are set to 0.
	 */
	public Buffer(MemorySegment s) {
		if (s == null || s.isFree()) {
			throw new IllegalArgumentException("Memory nmust not be null or free.");
		}
		
		this.memory = s;
	}
	
	/**
	 * Clears all references to the used memory and returns the memory.
	 * 
	 * @return The memory segment used by this buffer.
	 */
	public MemorySegment dispose() {
		MemorySegment s = this.memory;
		this.memory = null;
		return s;
	}
	
	/**
	 * An input buffer for IOReadableWritable objects.
	 * 
	 * @author Alexander Alexandrov
	 * @author Stephan Ewen
	 */
	public static final class Input extends Buffer {
		
		/**
		 * The input view around the memory from which is read.
		 */
		private final DataInputView inView;
		
		/**
		 * The current position in the buffer.
		 */
		private int position;
		
		/**
		 * The position for repeated reads.
		 */
		private int repeatPosition;
		
		/**
		 * Limit of the contents in the underlying memory.
		 */
		private int limit;
		
		
		public Input(MemorySegment memory) {
			super(memory);
			
			this.inView = memory.inputView;
			this.inView.reset();
		}
		
		/**
		 * Gets the current position of this buffer.
		 * 
		 * @return THe current position.
		 */
		public final int getPosition() {
			return this.position;
		}
		
		/**
		 * Gets the number of bytes that remain in this buffer, which is the difference between the buffer's
		 * limit and its current position.
		 * 
		 * @return The number of remaining bytes.
		 */
		public final int getRemainingBytes() {
			return this.limit - this.position;
		}
		
		/**
		 * Rewinds this buffer, such that the position is zero and the limit is kept.
		 */
		public final void rewind() {
			this.position = 0;
			this.repeatPosition = 0;
			this.inView.reset();
		}
		
		/**
		 * Resets the buffer such that the position is zero and the limit is set to the given value.
		 * 
		 * @param limit The new limit.
		 */
		public final void reset(int limit) {
			if (limit < 0) {
				throw new IllegalArgumentException();
			}
			rewind();
			this.limit = limit;
		}
		

		/**
		 * Reads an {@code IOReadableWritable} from the underlying memory
		 * segment into the provided {@link IOReadableWritable} object by
		 * calling it's {@link IOReadableWritable#read(DataInput)} operation
		 * with the backing memory's {@link DataInputView}.
		 * 
		 * @param object The object to read from the buffer.
		 * @return True, if the read was successful, false otherwise.
		 */
		public boolean read(IOReadableWritable object) {

			final int pos = this.position;
			
			try {
				if (pos >= this.limit) {
					return false;
				}
				
				object.read(inView); // read object
				
				final int newPos = this.inView.getPosition();
				
				if (newPos > limit) {
					return false;
				}
				
				// ok if read limit was not exceeded while reading
				this.repeatPosition = pos; // update position for repeated read
				this.position = newPos;
				
				return true;
			}
			catch (IOException e) {
				// I/O exception indicates unsuccessful read due to end of buffer
				// we need to set the input view back to that position where it tried to read
				this.inView.setPosition(pos);
				return false;
			}
		}
		
		/**
		 * Reads the most recently read {@code IOReadableWritable} from the underlying memory
		 * segment again into the provided {@link IOReadableWritable} object by
		 * calling it's {@link IOReadableWritable#read(DataInput)} operation 
		 * with the backing memory's {@link DataInputView}.
		 * 
		 * @param object The object to read from the buffer.
		 * @return True, if the read was successful, false otherwise.
		 */
		public boolean repeatRead(IOReadableWritable object) {

			try {
				if (this.repeatPosition >= this.limit) {
					return false;
				}
				this.inView.setPosition(repeatPosition);
				object.read(this.inView); // read object
				return true;
			}
			catch (IOException e) {
				throw new RuntimeException("This should not happen: A repeated read failed when the initial read succeeded.");
			}
		}
		
		/**
		 * Copies the bytes that remain in the buffer into the given array.
		 * 
		 * @param target The target array to copy the remaining bytes into.
		 * @throws IOException Thrown, if the input buffer is already exhausted.
		 * @throws ArrayIndexOutOfBoundsException Thrown, if the target array is too small for the remaining bytes.
		 */
		public void copyRemainingBytes(byte[] target) throws IOException {
			this.inView.readFully(target, 0, getRemainingBytes());
			this.position = this.inView.getPosition();
		}
		
		/**
		 * Reads the next byte from this input.
		 * 
		 * @throws IOException Thrown, if the input buffer is exhausted.
		 * @throws ArrayIndexOutOfBoundsException Thrown, if the target array is too small for the remaining bytes.
		 */
		public int getNextByte() throws IOException {
			if (this.position >= this.limit) {
				throw new EOFException();
			}
			
			int b = this.inView.readByte();
			b &= 0xff;
			this.position = this.inView.getPosition();
			return b;
		}

		/**
		 * Reads the buffer from a Java NIO {@link java.nio.FileChannel}.
		 * 
		 * @param channel The NIO file channel to read from
		 * @throws IOException Thrown by the {@link FileChannel#read(java.nio.ByteBuffer)} method.
		 */
		public void readFromChannel(FileChannel channel) throws IOException
		{
			final long bytesRemaining = channel.size() - channel.position();
			
			// check if the channel is already exhausted
			if (bytesRemaining < 1) {
				this.position = 0;
				this.limit = 0;
				return;
			}
			
			final int toRead = (int) Math.min(this.memory.size(), bytesRemaining);
			
			int bytesRead = channel.read(memory.wrap(0, toRead));
			
			this.position = 0;
			this.limit = bytesRead;
			this.inView.reset();
		}
	}

	/**
	 * An output buffer for {@code IOReadableWritable} objects.
	 * 
	 * @author Alexander Alexandrov
	 */
	public static final class Output extends Buffer {

		private final DataOutputView outView;
		
		public Output(MemorySegment memory) {
			super(memory);
			
			this.outView = memory.outputView;
			this.outView.reset();
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
			final int pos = this.outView.getPosition();
			try {
				object.write(this.outView);
				return true;
			} catch (IOException e) {
				this.outView.setPosition(pos);
				return false;
			}
		}

		/**
		 * Writes all written objects to the provided Java NIO {@link FileChannel}.
		 * 
		 * @param channel
		 * @throws IOException
		 */
		public void writeToChannel(FileChannel channel) throws IOException
		{
			channel.write(this.memory.wrap(0, this.outView.getPosition()));
		}
		
		/**
		 * Resets this output buffer by setting the position to the beginning.
		 */
		public void rewind() {
			this.outView.reset();
		}
		
		/**
		 * Gets the current position from this buffer.
		 * 
		 * @return The current position;
		 */
		public int getPosition()
		{
			return this.outView.getPosition();
		}
	}

	// -------------------------------------------------------------------------
	// Typing
	// -------------------------------------------------------------------------

	/**
	 * Enumeration class for the different buffer types.
	 * 
	 * @author Alexander Alexandrov
	 * @param <T> The buffer class identified by the {@code Type} instance
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
