/**
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


package org.apache.flink.core.memory;


import java.io.DataOutput;
import java.io.IOException;


/**
 * This interface defines a view over some memory that can be used to write contents to the memory. The view is
 * typically backed by one or more {@link org.apache.flink.core.memory.MemorySegment}. In most implementations the
 * {@link #tell()} and {@link #seek(long)} methods can only be called when the view has previously been locked. Locking
 * a view tells the system to keep underlying memory segments available instead of releasing them immendiately so that
 * you can seek back to earlier positions. Most implementations do not allow seeking lower than the position at which
 * {@link #lock()} was called.
 */
public interface DataOutputView extends DataOutput {
	
	/**
	 * Skips {@code numBytes} bytes memory. If some program reads the memory that was skipped over, the
	 * results are undefined. 
	 * 
	 * @param numBytes The number of bytes to skip.
	 * 
	 * @throws IOException Thrown, if any I/O related problem occurred such that the view could not
	 *                     be advanced to the desired position.
	 */
	public void skipBytesToWrite(int numBytes) throws IOException;
	
	/**
	 * Copies {@code numBytes} bytes from the source to this view.
	 * 
	 * @param source The source to copy the bytes from.
	 * @param numBytes The number of bytes to copy.
	 * 
	 * @throws IOException Thrown, if any I/O related problem occurred, such that either the input view
	 *                     could not be read, or the output could not be written.
	 */
	public void write(DataInputView source, int numBytes) throws IOException;

	/**
	 * Tells the view to keep the underlying buffers (if any) and allow calling of {@link #seek(long)} and {@link
	 * #tell()}.
	 */
	public void lock();

	/**
	 * Tells the system to release all underlying buffers (if any). Data written after the current writing position
	 * might be discarded or subsequently overwritten.
	 *
	 * @throws IOException
	 */
	public void unlock() throws IOException;

	/**
	 * Returns the current writing position. The 0 position is not necessarily the first position in the output but
	 * positions are consistent between calls to {@link #lock()} and {@link #unlock()}.
	 *
	 * Most implementations only allow calling this method after calling {@link #lock()}.
	 *
	 * @throws IOException
	 */
	public long tell() throws IOException;

	/**
	 * Sets the current writing position. The 0 position is not necessarily the first position in the output but
	 * positions are consistent between calls to {@link #lock()} and {@link #unlock()}.
	 *
	 * Most implementations only allow calling this method after calling {@link #lock()}.
	 *
	 * @throws IOException
	 */
	public void seek(long position) throws IOException;
}
