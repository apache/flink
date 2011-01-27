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

import java.nio.ByteBuffer;

public abstract class MemorySegment
{
	public final RandomAccessView randomAccessView;

	public final DataInputView inputView;

	public final DataOutputView outputView;

	protected final int size;

	protected boolean isFreed;

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	public MemorySegment(int size, RandomAccessView randomAccessView, 
			DataInputView inputView, DataOutputView outputView)
	{
		this.randomAccessView = randomAccessView;
		this.inputView = inputView;
		this.outputView = outputView;
		
		this.size = size;
		
		this.isFreed = false;
	}

	// -------------------------------------------------------------------------
	// MemorySegment
	// -------------------------------------------------------------------------
	
	public final int size() {
		return size;
	}

	public final void free() {
		isFreed = true;
	}

	public final boolean isFree() {
		return isFreed;
	}
	
	// -------------------------------------------------------------------------
	// Helper methods
	// -------------------------------------------------------------------------
	

	/**
	 * Wraps the chunk of the underlying memory located between {@code offset} and {@code length} in a NIO ByteBuffer.
	 * 
	 * @param offset
	 * @param length
	 * @return
	 * @throws IndexOutOfBoundsException
	 */
	abstract public ByteBuffer wrap(int offset, int length);
}
