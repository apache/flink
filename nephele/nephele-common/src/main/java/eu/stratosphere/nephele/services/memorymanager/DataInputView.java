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

package eu.stratosphere.nephele.services.memorymanager;


import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;


/**
 * This interface defines a view over some memory that can be used to sequentially read the contents of the memory.
 * The view is typically backed by one or more {@link eu.stratosphere.nephele.services.memorymanager.MemorySegment}.
 *
 * @author Alexander Alexandrov
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface DataInputView extends DataInput
{
	/**
	 * Skips {@code numBytes} bytes of memory. In contrast to the {@link #skipBytes(int)} method,
	 * this method always skips the desired number of bytes or throws an {@link java.io.EOFException}.
	 * 
	 * @param numBytes The number of bytes to skip.
	 * 
	 * @throws EOFException Thrown, when less then {@code numBytes} remain in the input.
	 * @throws IOException Thrown, if any I/O related problem occurred such that the input could not
	 *                     be advanced to the desired position.
	 */
	public void skipBytesToRead(int numBytes) throws IOException;
}
