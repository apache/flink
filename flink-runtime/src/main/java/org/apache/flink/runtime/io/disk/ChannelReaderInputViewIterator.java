/*
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


package org.apache.flink.runtime.io.disk;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.util.MutableObjectIterator;


/**
 * A simple iterator over the input read though an I/O channel.
 *
 */
public class ChannelReaderInputViewIterator<E> implements MutableObjectIterator<E>
{
	private final ChannelReaderInputView inView;
	
	private final TypeSerializer<E> accessors;
	
	private final List<MemorySegment> freeMemTarget;
	
	
	public ChannelReaderInputViewIterator(IOManager ioAccess, FileIOChannel.ID channel, List<MemorySegment> segments,
			List<MemorySegment> freeMemTarget, TypeSerializer<E> accessors, int numBlocks)
	throws IOException
	{
		this(ioAccess, channel, new LinkedBlockingQueue<MemorySegment>(), segments, freeMemTarget, accessors, numBlocks);
	}
		
	public ChannelReaderInputViewIterator(IOManager ioAccess, FileIOChannel.ID channel,  LinkedBlockingQueue<MemorySegment> returnQueue,
			List<MemorySegment> segments, List<MemorySegment> freeMemTarget, TypeSerializer<E> accessors, int numBlocks)
	throws IOException
	{
		this(ioAccess.createBlockChannelReader(channel, returnQueue), returnQueue,
			segments, freeMemTarget, accessors, numBlocks);
	}
		
	public ChannelReaderInputViewIterator(BlockChannelReader<MemorySegment> reader, LinkedBlockingQueue<MemorySegment> returnQueue,
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
	public E next(E reuse) throws IOException
	{
		try {
			return this.accessors.deserialize(reuse, this.inView);
		} catch (EOFException eofex) {
			final List<MemorySegment> freeMem = this.inView.close();
			if (this.freeMemTarget != null) {
				this.freeMemTarget.addAll(freeMem);
			}
			return null;
		}
	}

	@Override
	public E next() throws IOException
	{
		try {
			return this.accessors.deserialize(this.inView);
		} catch (EOFException eofex) {
			final List<MemorySegment> freeMem = this.inView.close();
			if (this.freeMemTarget != null) {
				this.freeMemTarget.addAll(freeMem);
			}
			return null;
		}
	}
}
