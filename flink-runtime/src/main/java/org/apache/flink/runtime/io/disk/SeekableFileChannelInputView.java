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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MathUtils;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link org.apache.flink.core.memory.DataInputView} that is backed by a {@link BlockChannelReader},
 * making it effectively a data input stream. The view reads it data in blocks from the underlying channel.
 * The view can read data that has been written by a {@link FileChannelOutputView}, or that was written in blocks
 * in another fashion.
 */
public class SeekableFileChannelInputView extends AbstractPagedInputView {
	
	private BlockChannelReader<MemorySegment> reader;
	
	private final IOManager ioManager;
	
	private final FileIOChannel.ID channelId;
	
	private final MemoryManager memManager;
	
	private final List<MemorySegment> memory;
	
	private final int sizeOfLastBlock;
	
	private final int numBlocksTotal;
	
	private final int segmentSize;
	
	private int numRequestsRemaining;
	
	private int numBlocksRemaining;
	
	// --------------------------------------------------------------------------------------------
	
	public SeekableFileChannelInputView(IOManager ioManager, FileIOChannel.ID channelId, MemoryManager memManager, List<MemorySegment> memory, int sizeOfLastBlock) throws IOException {
		super(0);
		
		checkNotNull(ioManager);
		checkNotNull(channelId);
		checkNotNull(memManager);
		checkNotNull(memory);
		
		this.ioManager = ioManager;
		this.channelId = channelId;
		this.memManager = memManager;
		this.memory = memory;
		this.sizeOfLastBlock = sizeOfLastBlock;
		this.segmentSize = memManager.getPageSize();
		
		this.reader = ioManager.createBlockChannelReader(channelId);
		
		try {
			final long channelLength = reader.getSize();
			
			final int blockCount =  MathUtils.checkedDownCast(channelLength / segmentSize);
			this.numBlocksTotal = (channelLength % segmentSize == 0) ? blockCount : blockCount + 1;

			this.numBlocksRemaining = this.numBlocksTotal;
			this.numRequestsRemaining = numBlocksRemaining;
			
			for (int i = 0; i < memory.size(); i++) {
				sendReadRequest(memory.get(i));
			}
			
			advance();
		}
		catch (IOException e) {
			memManager.release(memory);
			throw e;
		}
	}
	
	public void seek(long position) throws IOException {
		final int block = MathUtils.checkedDownCast(position / segmentSize);
		final int positionInBlock = (int) (position % segmentSize);
		
		if (position < 0 || block >= numBlocksTotal || (block == numBlocksTotal - 1 && positionInBlock > sizeOfLastBlock)) {
			throw new IllegalArgumentException("Position is out of range");
		}
		
		clear();
		if (reader != null) {
			reader.close();
		}
		
		reader = ioManager.createBlockChannelReader(channelId);
		
		if (block > 0) {
			reader.seekToPosition(((long) block) * segmentSize);
		}
		
		this.numBlocksRemaining = this.numBlocksTotal - block;
		this.numRequestsRemaining = numBlocksRemaining;
		
		for (int i = 0; i < memory.size(); i++) {
			sendReadRequest(memory.get(i));
		}
		
		numBlocksRemaining--;
		seekInput(reader.getNextReturnedBlock(), positionInBlock, numBlocksRemaining == 0 ? sizeOfLastBlock : segmentSize);
	}
	
	public void close() throws IOException {
		close(false);
	}
	
	public void closeAndDelete() throws IOException {
		close(true);
	}
	
	private void close(boolean deleteFile) throws IOException {
		try {
			clear();
			if (deleteFile) {
				reader.closeAndDelete();
			} else {
				reader.close();
			}
		} finally {
			synchronized (memory) {
				memManager.release(memory);
				memory.clear();
			}
		}
	}
	
	@Override
	protected MemorySegment nextSegment(MemorySegment current) throws IOException {
		// check for end-of-stream
		if (numBlocksRemaining <= 0) {
			reader.close();
			throw new EOFException();
		}
		
		// send a request first. if we have only a single segment, this same segment will be the one obtained in the next lines
		if (current != null) {
			sendReadRequest(current);
		}
		
		// get the next segment
		numBlocksRemaining--;
		return reader.getNextReturnedBlock();
	}
	
	@Override
	protected int getLimitForSegment(MemorySegment segment) {
		return numBlocksRemaining > 0 ? segment.size() : sizeOfLastBlock;
	}
	
	private void sendReadRequest(MemorySegment seg) throws IOException {
		if (numRequestsRemaining > 0) {
			reader.readBlock(seg);
			numRequestsRemaining--;
		}
	}
}
