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

package eu.stratosphere.nephele.io.channels;

import java.nio.ByteBuffer;

import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPool;

/**
 * The memory buffer pool connector provides a connection between {@link MemoryBuffer} and the {@link LocalBufferPool}
 * the memory buffer's encapsulated byte buffer has originally been taken from.
 * 
 * @author warneke
 */
public interface MemoryBufferPoolConnector {

	/**
	 * Called by the {@link MemoryBufferRecycler} to return a buffer to its original buffer pool.
	 * 
	 * @param byteBuffer
	 *        the buffer to be recycled
	 */
	void recycle(ByteBuffer byteBuffer);
}
