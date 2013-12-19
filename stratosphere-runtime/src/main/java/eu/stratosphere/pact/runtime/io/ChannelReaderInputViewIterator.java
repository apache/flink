/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.io;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.iomanager.BlockChannelReader;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.ChannelReaderInputView;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.util.MutableObjectIterator;


/**
 * A simple iterator over the input read though an I/O channel.
 *
 */
public class ChannelReaderInputViewIterator<E> implements MutableObjectIterator<E>
{
	private final ChannelReaderInputView inView;
	
	private final TypeSerializer<E> accessors;
	
	private final List<MemorySegment> freeMemTarget;
	
	
	public ChannelReaderInputViewIterator(IOManager ioAccess, Channel.ID channel, List<MemorySegment> segments,
			List<MemorySegment> freeMemTarget, TypeSerializer<E> accessors, int numBlocks)
	throws IOException
	{
		this(ioAccess, channel, new LinkedBlockingQueue<MemorySegment>(), segments, freeMemTarget, accessors, numBlocks);
	}
		
	public ChannelReaderInputViewIterator(IOManager ioAccess, Channel.ID channel,  LinkedBlockingQueue<MemorySegment> returnQueue,
			List<MemorySegment> segments, List<MemorySegment> freeMemTarget, TypeSerializer<E> accessors, int numBlocks)
	throws IOException
	{
		this(ioAccess.createBlockChannelReader(channel, returnQueue), returnQueue,
			segments, freeMemTarget, accessors, numBlocks);
	}
		
	public ChannelReaderInputViewIterator(BlockChannelReader reader, LinkedBlockingQueue<MemorySegment> returnQueue,
			List<MemorySegment> segments, List<MemorySegment> freeMemTarget, TypeSerializer<E> accessors, int numBlocks)
	throws IOException
	{
		this.accessors = accessors;
		this.freeMemTarget = freeMemTarget;
		this.inView = new ChannelReaderInputView(reader, segments, numBlocks, false);
	}
	
	public ChannelReaderInputViewIterator(ChannelReaderInputView inView, List<MemorySegment> freeMemTarget, TypeSerializer<E> accessors)
	{
		this.inView = inView;
		this.freeMemTarget = freeMemTarget;
		this.accessors = accessors;
	}
			


	@Override
	public boolean next(E target) throws IOException
	{
		try {
			this.accessors.deserialize(target, this.inView);
			return true;
		} catch (EOFException eofex) {
			final List<MemorySegment> freeMem = this.inView.close();
			if (this.freeMemTarget != null) {
				this.freeMemTarget.addAll(freeMem);
			}
			return false;
		}
	}
}
