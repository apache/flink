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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.core.memory.MemorySegmentSource;
import eu.stratosphere.nephele.services.iomanager.BlockChannelReader;
import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.iomanager.HeaderlessChannelReaderInputView;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.AbstractPagedOutputView;


/**
 * An output view that buffers written data in memory pages and spills them when they are full.
 */
public class SpillingBuffer extends AbstractPagedOutputView {
	
	private final ArrayList<MemorySegment> fullSegments;
	
	private final MemorySegmentSource memorySource;
	
	private BlockChannelWriter writer;
	
	private RandomAccessInputView inMemInView;
	
	private HeaderlessChannelReaderInputView externalInView;
	
	private final IOManager ioManager;
	
	private int blockCount;
	
	private int numBytesInLastSegment;
	
	private int numMemorySegmentsInWriter;


	public SpillingBuffer(IOManager ioManager, MemorySegmentSource memSource, int segmentSize) {
		super(memSource.nextSegment(), segmentSize, 0);
		
		this.fullSegments = new ArrayList<MemorySegment>(16);
		this.memorySource = memSource;
		this.ioManager = ioManager;
	}
	

	@Override
	protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException {
		// check if we are still in memory
		if (this.writer == null) {
			this.fullSegments.add(current);
			
			final MemorySegment nextSeg = this.memorySource.nextSegment();
			if (nextSeg != null) {
				return nextSeg;
			} else {
				// out of memory, need to spill: create a writer
				this.writer = this.ioManager.createBlockChannelWriter(this.ioManager.createChannel());
				
				// add all segments to the writer
				this.blockCount = this.fullSegments.size();
				this.numMemorySegmentsInWriter = this.blockCount;
				for (int i = 0; i < this.fullSegments.size(); i++) {
					this.writer.writeBlock(this.fullSegments.get(i));
				}
				this.fullSegments.clear();
				final MemorySegment seg = this.writer.getNextReturnedSegment();
				this.numMemorySegmentsInWriter--;
				return seg;
			}
		} else {
			// spilling
			this.writer.writeBlock(current);
			this.blockCount++;
			return this.writer.getNextReturnedSegment();
		}
	}
	
	public DataInputView flip() throws IOException {
		// check whether this is the first flip and we need to add the current segment to the full ones
		if (getCurrentSegment() != null) {
			// first flip
			if (this.writer == null) {
				// in memory
				this.fullSegments.add(getCurrentSegment());
				this.numBytesInLastSegment = getCurrentPositionInSegment();
				this.inMemInView = new RandomAccessInputView(this.fullSegments, this.segmentSize, this.numBytesInLastSegment);
			} else {
				// external: write the last segment and collect the memory back
				this.writer.writeBlock(this.getCurrentSegment());
				this.numMemorySegmentsInWriter++;
				
				this.numBytesInLastSegment = getCurrentPositionInSegment();
				this.blockCount++;
				this.writer.close();
				for (int i = this.numMemorySegmentsInWriter; i > 0; i--) {
					this.fullSegments.add(this.writer.getNextReturnedSegment());
				}
				this.numMemorySegmentsInWriter = 0;
			}
			
			// make sure we cannot write more
			clear();
		}
		
		if (this.writer == null) {
			// in memory
			this.inMemInView.setReadPosition(0);
			return this.inMemInView;
		} else {
			// recollect memory from a previous view
			if (this.externalInView != null) {
				this.externalInView.close();
			}
			
			final BlockChannelReader reader = this.ioManager.createBlockChannelReader(this.writer.getChannelID());
			this.externalInView = new HeaderlessChannelReaderInputView(reader, this.fullSegments, this.blockCount, this.numBytesInLastSegment, false);
			return this.externalInView;
		}
	}
	
	/**
	 * @return A list with all memory segments that have been taken from the memory segment source.
	 */
	public List<MemorySegment> close() throws IOException {
		final ArrayList<MemorySegment> segments = new ArrayList<MemorySegment>(this.fullSegments.size() + this.numMemorySegmentsInWriter);
		
		// if the buffer is still being written, clean that up
		if (getCurrentSegment() != null) {
			segments.add(getCurrentSegment());
			clear();
		}
		
		moveAll(this.fullSegments, segments);
		this.fullSegments.clear();
		
		// clean up the writer
		if (this.writer != null) {
			// closing before the first flip, collect the memory in the writer
			this.writer.close();
			for (int i = this.numMemorySegmentsInWriter; i > 0; i--) {
				segments.add(this.writer.getNextReturnedSegment());
			}
			this.writer.closeAndDelete();
			this.writer = null;
		}
		
		// clean up the views
		if (this.inMemInView != null) {
			this.inMemInView = null;
		}
		if (this.externalInView != null) {
			if (!this.externalInView.isClosed()) {
				this.externalInView.close();
			}
			this.externalInView = null;
		}
		return segments;
	}
	
	/**
	 * Utility method that moves elements. It avoids copying the data into a dedicated array first, as
	 * the {@link ArrayList#addAll(java.util.Collection)} method does.
	 * 
	 * @param <E>
	 * @param source
	 * @param target
	 */
	private static final <E> void moveAll(ArrayList<E> source, ArrayList<E> target) {
		target.ensureCapacity(target.size() + source.size());
		for (int i = source.size() - 1; i >= 0; i--) {
			target.add(source.remove(i));
		}
	}
}
