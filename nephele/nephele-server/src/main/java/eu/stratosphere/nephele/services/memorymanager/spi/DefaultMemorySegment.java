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

package eu.stratosphere.nephele.services.memorymanager.spi;

import java.nio.ByteBuffer;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.RandomAccessView;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager.MemorySegmentDescriptor;

public final class DefaultMemorySegment extends MemorySegment
{
	protected final MemorySegmentDescriptor descriptor;

	public DefaultMemorySegment(MemorySegmentDescriptor descriptor,
			RandomAccessView randomAccessView, DataInputView inputView, DataOutputView outputView)
	{
		super(descriptor.size, randomAccessView, inputView, outputView);
		this.descriptor = descriptor;
	}
	
	@Override
	public ByteBuffer wrap(int offset, int length) {
		if (offset > size || offset + length > size) {
			throw new IndexOutOfBoundsException();
		}

		return ByteBuffer.wrap(descriptor.memory, descriptor.start + offset, length);
	}
	
	public MemorySegmentDescriptor getSegmentDescriptor()
	{
		return this.descriptor;
	}
}
