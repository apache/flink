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

import java.io.IOException;
import java.util.List;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * Writer interface for a writer with a stream abstraction. The writer is typically created with a collection
 * of MemorySegments which it internally uses for write-behind logic.
 * 
 * @author Erik Nijkamp
 */
public interface Writer
{
	/**
	 * Closes the writer. Makes sure all buffered and unwritten data is written and then returns the
	 * memory segments used by the writer.
	 * 
	 * @return The memory segments used internally.
	 * @throws IOException Thrown, if the writing of buffered data fails.
	 */
	List<MemorySegment> close() throws IOException;

	/**
	 * Writes an object to the stream behind the writer. This function typically writes the object into a buffer,
	 * which is eventually flushed to disk, when full or when the writer is closed.
	 *  
	 * @param readable The object to be written.
	 * @return True, if the object was successfully written, false otherwise.
	 * @throws IOException Thrown, if the writing process encounters an I/O related error.
	 */
	boolean write(IOReadableWritable readable) throws IOException;
}
