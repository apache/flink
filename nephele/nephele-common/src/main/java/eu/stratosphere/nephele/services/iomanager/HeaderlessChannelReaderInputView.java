/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;


/**
 * A {@link DataInputView} that is backed by a {@link BlockChannelReader}, making it effectively a data input
 * stream. This view is similar to the {@link ChannelReaderInputView}, but does not expect
 * a header for each block, giving a direct stream abstraction over sequence of written
 * blocks. It therefore requires specification of the number of blocks and the number of
 * bytes in the last block.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class HeaderlessChannelReaderInputView extends ChannelReaderInputView
{
	private int numBlocksRemaining;		// the number of blocks not yet consumed
	
	private final int lastBlockBytes;	// the number of valid bytes in the last block

	/**
	 * Creates a new channel reader that reads from the given channel, expecting a specified
	 * number of blocks in the channel, and returns only a specified number of bytes from
	 * the last block.
	 * <p>
	 * WARNING: If the number of blocks given here is higher than the number of blocks in the
	 * channel, then the last blocks will not be filled by the reader and will contain
	 * undefined data.
	 * 
	 * @param reader The reader that reads the data from disk back into memory.
	 * @param memory A list of memory segments that the reader uses for reading the data in. If the
	 *               list contains more than one segment, the reader will asynchronously pre-fetch
	 *               blocks ahead.
	 * @param numBlocks The number of blocks this channel will read.
	 * @param numBytesInLastBlock The number of valid bytes in the last block.
	 * @param waitForFirstBlock A flag indicating weather this constructor call should block
	 *                          until the first block has returned from the asynchronous I/O reader.
	 * 
	 * @throws IOException Thrown, if the read requests for the first blocks fail to be
	 *                     served by the reader.
	 */
	public HeaderlessChannelReaderInputView(BlockChannelReader reader, List<MemorySegment> memory, int numBlocks,
			int numBytesInLastBlock, boolean waitForFirstBlock)
	throws IOException
	{
		super(reader, memory, numBlocks, 0, waitForFirstBlock);
		
		this.numBlocksRemaining = numBlocks;
		this.lastBlockBytes = numBytesInLastBlock;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.io.ChannelReaderInputView#nextSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
	 */
	@Override
	protected MemorySegment nextSegment(MemorySegment current) throws IOException
	{
		// check for end-of-stream
		if (this.numBlocksRemaining <= 0) {
			this.reader.close();
			throw new EOFException();
		}
		
		// send a request first. if we have only a single segment, this same segment will be the one obtained in
		// the next lines
		if (current != null) {
			sendReadRequest(current);
		}
		
		// get the next segment
		this.numBlocksRemaining--;
		return this.reader.getNextReturnedSegment();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.io.AbstractPagedInputView#getLimitForSegment(eu.stratosphere.nephele.services.memorymanager.MemorySegment)
	 */
	@Override
	protected int getLimitForSegment(MemorySegment segment)
	{
		return this.numBlocksRemaining > 0 ? segment.size() : this.lastBlockBytes;
	}
}
