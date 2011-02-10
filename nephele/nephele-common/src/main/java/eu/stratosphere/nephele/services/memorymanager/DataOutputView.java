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


/**
 * This interface defines a view over a {@link eu.stratosphere.nephele.services.memorymanager.MemorySegment} that
 * can be used to sequentially write to the memory.
 *
 * @author Alexander Alexandrov
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface DataOutputView extends DataOutput
{
	/**
	 * Gets the current read position.
	 * 
	 * @return The current read position.
	 */
	public int getPosition();

	/**
	 * Sets the current read position.
	 */
	public DataOutput setPosition(int position);

	/**
	 * Skips {@code size} memory.
	 */
	public DataOutputView skip(int size);

	/**
	 * Resets the position to zero.
	 */
	public DataOutputView reset();
}
