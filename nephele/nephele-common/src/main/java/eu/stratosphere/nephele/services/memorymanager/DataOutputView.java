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


import java.io.DataOutput;
import java.io.IOException;


/**
 * This interface defines a view over some memory that can be used to sequentially write contents to the memory.
 * The view is typically backed by one or more {@link eu.stratosphere.nephele.services.memorymanager.MemorySegment}.
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
}
