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

package eu.stratosphere.pact.runtime.sort;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import eu.stratosphere.nephele.services.iomanager.MemoryIOWrapper;
import eu.stratosphere.nephele.services.iomanager.RawComparator;
import eu.stratosphere.nephele.services.iomanager.Writer;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemoryBacked;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.UnboundMemoryBackedException;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultDataOutputView;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemorySegmentView;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Sortable buffer based on heap/stack concept where pairs are written in the heap and index into stack.
 * 
 * Caution (1): 
 *   - index is stored in stack (within memory segment)
 *   - thus Buffer.Input.read(...) == false will not work
 *   - take the BufferSortableGuaranteed.position as upper limit
 *   
 * Caution (2):
 *   - offset/order pointers are stored interleaved in the stack
 *   - order of index entries reflects the sequential write order
 *   - order of offset entries defines a total order (after sorting)
 * 
 * |        heap       |       empty      |                  stack                   |
 * | pair | pair | pair | ... | ... | ... | ... | offset (4 bytes) | index (8 bytes) |
 * 
 * TODO: Rewrite this class to work with collections of small buffers. The buffers will be concatenated to form
 * two lists: One containing the records (preceded by the record length), the other one containing normalized keys
 * and pointers.
 * 
 * @author Erik Nijkamp
 * @param <K>
 *        The type of the key.
 * @param <V>
 *        The type of the value.
 */
public final class BufferSortableGuaranteed extends MemoryBacked implements IndexedSortable {
	
	/**
	 * {@link OutputView} wrapper which can (virtually) reduce the size of the buffer segment
	 * after being instantiated. Remember, we write the index as stack (reverse) thus
	 * the size of the buffer portion for pairs reduces with each additional index entry.
	 * 
	 * Guarantees that the index is not overwritten by pairs.
	 * 
	 * For reasons of efficiency (avoiding another layer of invocations) this code is
	 * mostly equivalent to {@link DefaultDataOutputView}
	 */
	private static final class HeapStackDataOutputView extends DefaultMemorySegmentView implements DataOutputView {
		
		/**
		 * The current write size.
		 */
		private int position;
		
		/**
		 * The current (reversed) stack end.
		 */
		private int stackEndAbs;
		
		// -------------------------------------------------------------------------
		// Offsets
		// -------------------------------------------------------------------------
		
		public void growStack(int bytes)
		{
			this.stackEndAbs -= bytes;
		}
		
		public int getStackEndRel()
		{
			return this.stackEndAbs - this.offset;
		}
		
		public int getHeapEndRel()
		{
			return this.position - this.offset;
		}

		// -------------------------------------------------------------------------
		// Constructors
		// -------------------------------------------------------------------------

		public HeapStackDataOutputView(byte[] memory, int offset, int size) {
			super(memory, offset, size);
			resetStackHeap();
		}
		
		public void resetStackHeap()
		{
			this.position = this.offset;
			this.stackEndAbs = this.offset + this.size;
		}

		// -------------------------------------------------------------------------
		// DataOutputView
		// -------------------------------------------------------------------------

		@Override
		public int getPosition() {
			return this.position - this.offset;
		}

		@Override
		public DataOutputView setPosition(int position) {
			this.position = position + this.offset;
			return this;
		}

		@Override
		public DataOutputView skip(int size) {
			this.position += size;
			return this;
		}

		@Override
		public DataOutputView reset() {
			this.position = this.offset;
			return this;
		}

		// ------------------------------------------------------------------------
		// DataOutput
		// ------------------------------------------------------------------------

		@Override
		public void write(int b) throws IOException {
			if (this.position < this.stackEndAbs) {
				this.memory[this.position++] = (byte) (b & 0xff);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void write(byte[] b) throws IOException {
			write(b, 0, b.length);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			if (this.position <= this.stackEndAbs - len && off <= b.length - len) {
				System.arraycopy(b, off, this.memory, this.position, len);
				this.position += len;
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeBoolean(boolean v) throws IOException {
			if (this.position < this.stackEndAbs) {
				this.memory[this.position++] = (byte) (v ? 1 : 0);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeByte(int v) throws IOException {
			write(v);
		}

		@Override
		public void writeBytes(String s) throws IOException {
			if (this.position < this.stackEndAbs - s.length()) {
				int length = s.length();
				for (int i = 0; i < length; i++) {
					writeByte(s.charAt(i));
				}
				this.position += length;
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeChar(int v) throws IOException {

			if (this.position < this.stackEndAbs - 1) {
				this.memory[this.position++] = (byte) ((v >> 8) & 0xff);
				this.memory[this.position++] = (byte) ((v >> 0) & 0xff);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeChars(String s) throws IOException {

			if (this.position < this.stackEndAbs - 2 * s.length()) {
				int length = s.length();
				for (int i = 0; i < length; i++) {
					writeChar(s.charAt(i));
				}
			} else {
				throw new EOFException();
			}

		}

		@Override
		public void writeDouble(double v) throws IOException {
			writeLong(Double.doubleToLongBits(v));
		}

		@Override
		public void writeFloat(float v) throws IOException {
			writeInt(Float.floatToIntBits(v));
		}

		@Override
		public void writeInt(int v) throws IOException {
			if (this.position < this.stackEndAbs - 3) {
				this.memory[this.position++] = (byte) ((v >> 24) & 0xff);
				this.memory[this.position++] = (byte) ((v >> 16) & 0xff);
				this.memory[this.position++] = (byte) ((v >> 8) & 0xff);
				this.memory[this.position++] = (byte) ((v >> 0) & 0xff);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeLong(long v) throws IOException {
			if (this.position < this.stackEndAbs - 7) {
				this.memory[this.position++] = (byte) ((v >> 56) & 0xff);
				this.memory[this.position++] = (byte) ((v >> 48) & 0xff);
				this.memory[this.position++] = (byte) ((v >> 40) & 0xff);
				this.memory[this.position++] = (byte) ((v >> 32) & 0xff);
				this.memory[this.position++] = (byte) ((v >> 24) & 0xff);
				this.memory[this.position++] = (byte) ((v >> 16) & 0xff);
				this.memory[this.position++] = (byte) ((v >> 8) & 0xff);
				this.memory[this.position++] = (byte) ((v >> 0) & 0xff);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeShort(int v) throws IOException {
			if (this.position < this.stackEndAbs - 1) {
				this.memory[this.position++] = (byte) ((v >>> 8) & 0xff);
				this.memory[this.position++] = (byte) ((v >>> 0) & 0xff);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public void writeUTF(String str) throws IOException {
			int strlen = str.length();
			int utflen = 0;
			int c, count = 0;

			/* use charAt instead of copying String to char array */
			for (int i = 0; i < strlen; i++) {
				c = str.charAt(i);
				if ((c >= 0x0001) && (c <= 0x007F)) {
					utflen++;
				} else if (c > 0x07FF) {
					utflen += 3;
				} else {
					utflen += 2;
				}
			}

			if (utflen > 65535)
				throw new UTFDataFormatException("Encoded string is too long: " + utflen);

			byte[] bytearr = new byte[utflen + 2];

			bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
			bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);

			int i = 0;
			for (i = 0; i < strlen; i++) {
				c = str.charAt(i);
				if (!((c >= 0x0001) && (c <= 0x007F)))
					break;
				bytearr[count++] = (byte) c;
			}

			for (; i < strlen; i++) {
				c = str.charAt(i);
				if ((c >= 0x0001) && (c <= 0x007F)) {
					bytearr[count++] = (byte) c;

				} else if (c > 0x07FF) {
					bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
					bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
					bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
				} else {
					bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
					bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
				}
			}

			write(bytearr, 0, utflen + 2);
		}
	}

	// ------------------------------------------------------------------------
	//                              Constants
	// ------------------------------------------------------------------------

	private static final int OFFSET_LEN = 4;
	
	private static final int OFFSET_POSITION_LEN = 4; // bytes per offset
	
	private static final int STACK_ENTRY_SIZE = OFFSET_LEN + OFFSET_POSITION_LEN;
	

	// ------------------------------------------------------------------------
	//                               Members
	// ------------------------------------------------------------------------

	private final PactRecord record;
	
	private final MemoryIOWrapper memoryWrapper;

	private final RawComparator comparator;
	
	private HeapStackDataOutputView outputView;
	
	
	
	private int position;
	
	private int pairsCount;

	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	public BufferSortableGuaranteed(MemorySegment memory, RawComparator comparator)
	{
		super();
		
		this.comparator = comparator;
		this.record = new PactRecord();

		// bind memory segment
		bind(memory);
		this.memoryWrapper = new MemoryIOWrapper(memory);
	}

	// -------------------------------------------------------------------------
	// Memory Segment
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.nephele.services.memorymanager.MemoryBacked#bind(eu.stratosphere.nephele.services.memorymanager.
	 * MemorySegment)
	 */
	@Override
	public boolean bind(MemorySegment memory) {
		if (super.bind(memory)) {
			
			this.outputView = new HeapStackDataOutputView(memory.getBackingArray(),
				memory.translateOffset(0), memory.size());

			// reset counters
			reset();
			return true;
		} else {
			return false;
		}
	}

	public void reset() {
		// memory segment
		this.outputView.reset();

		// buffer
		this.position = 0;
		this.pairsCount = 0;

		// accounting
		this.outputView.resetStackHeap();
	}

	// -------------------------------------------------------------------------
	// Buffering
	// -------------------------------------------------------------------------

	public boolean isEmpty() {
		return this.outputView.getPosition() == 0;
	}

	public int getPosition() {
		return this.position;
	}
	
	public int getCapacity() {
		return this.outputView.getSize();
	}
	
	public int getOccupancy() {
		return this.outputView.getPosition() + (this.pairsCount * STACK_ENTRY_SIZE);
	}

	// -------------------------------------------------------------------------
	// Retrieving and Writing
	// -------------------------------------------------------------------------


//	public void getKey(K target, int logicalPosition) throws IOException
//	{
//		final int physicalPosition = readOffsetPosition(logicalPosition);
//		final int keyStart = readPairOffset(physicalPosition);
//		this.memory.inputView.setPosition(keyStart);
//		
//		this.keyDeserializer.deserialize(target);
//	}
	

	public void getRecord(PactRecord target, int logicalPosition) throws IOException
	{
		final int physicalPosition = readOffsetPosition(logicalPosition);
		final int start = readPairOffset(physicalPosition);
		this.memory.inputView.setPosition(start);
		
		target.read(this.memory.inputView);
	}

	/**
	 * Writes the provided key/value pair into the underlying memory segment.
	 * 
	 * @param pair
	 *        The key/value pair to be written.
	 * @throws IOException
	 * @throws UnboundMemoryBackedException
	 */
	public boolean write(PactRecord record) {
		try {
			// 1) serialize record into buffer
			final int offset = this.outputView.getPosition();
			record.write(this.outputView);
			
			// 2) INSERT INDEX AND OFFSET
			// a. check heap size
			final int free = this.outputView.getStackEndRel() - this.outputView.getHeapEndRel();
			if (free < STACK_ENTRY_SIZE) {
				return false;
			}
			
			// b. write offset (pointer into segment)
			this.outputView.growStack(OFFSET_LEN);
			this.memory.putInt(this.outputView.getStackEndRel(), offset);

			// c. write physical position (pointer to offset)
			final int physicalPos = outputView.getStackEndRel();
			this.outputView.growStack(OFFSET_POSITION_LEN);
			this.memory.putInt(this.outputView.getStackEndRel(), physicalPos);

			// 3). UPDATE WRITE POSITION
			this.pairsCount++;
			this.position = this.outputView.getPosition();
			return true;
		} 
		catch (IOException e) {
			return false;
		}
	}
	
	// ------------------------------------------------------------------------
	
	private final int readPairOffset(int physicalOffsetPosition)
	{
		return this.memory.getInt(physicalOffsetPosition);
	}
	
	private final void writeOffsetPosition(int logicalPosition, int offset)
	{
		final int stackoffset = (logicalPosition + 1) * STACK_ENTRY_SIZE;
		final int memoryoffset = this.outputView.getSize() - stackoffset;
		this.memory.putInt(memoryoffset, offset);
	}
	
	private final int readOffsetPosition(int logicalPosition)
	{
		final int stackoffset = (logicalPosition + 1) * STACK_ENTRY_SIZE;
		final int memoryoffset = this.outputView.getSize() - stackoffset;
		return this.memory.getInt(memoryoffset);
	}

	// ------------------------------------------------------------------------
	
	/**
	 * Writes this buffer completely to the given writer.
	 * 
	 * @param writer The writer to write the segment to.
	 * @throws IOException Thrown, if the writer caused an I/O exception.
	 */
	public void writeToChannel(final Writer writer) throws IOException {
		if (!isBound()) {
			new UnboundMemoryBackedException();
		}

		final MemoryIOWrapper memoryWrapper = new MemoryIOWrapper(this.memory);

		// write according to index
		for (int i = 0; i < size(); i++)
		{
			int offsetPosition = readOffsetPosition(i);

			// start and end within memory segment
			int kvstart = readPairOffset(offsetPosition);
			int kvend = 0;
			
			// for the last pair there is no next pair
			if(offsetPosition - STACK_ENTRY_SIZE > this.outputView.getStackEndRel()) {
				// -> kvend = kvstart of next pair
				kvend = readPairOffset(offsetPosition - STACK_ENTRY_SIZE);
			}
			else {
				kvend = this.position;
			}

			// set offset within memory segment
			final int kvlength = kvend - kvstart;
			memoryWrapper.setIOBlock(kvstart, kvlength);

			// copy serialized pair to writer
			writer.write(memoryWrapper);
		}
	}

	/**
	 * Writes a series of key/value pairs in this buffer to the given writer.
	 * 
	 * @param writer The writer to write the pairs to.
	 * @param start The position (logical number) of the first pair that is written.
	 * @param num The number of pairs to be written.
	 * @throws IOException Thrown, if the writer caused an I/O exception.
	 */
	public void writeToChannel(final Writer writer, final int start, final int num) throws IOException {
		// write according to index
		for (int i = start; i < start + num; i++) {
			// offset to index element
			int offsetPosition = readOffsetPosition(i);

			// start and end within memory segment
			int kvstart = readPairOffset(offsetPosition);
			int kvend = 0;
			
			// for the last pair there is no next pair
			if(offsetPosition - STACK_ENTRY_SIZE > this.outputView.getStackEndRel()) {
				// -> kvend = kvstart of next pair
				kvend = readPairOffset(offsetPosition - STACK_ENTRY_SIZE);
			}
			else {
				kvend = this.position;
			}

			// set offset within memory segment
			final int kvlength = kvend - kvstart;
			memoryWrapper.setIOBlock(kvstart, kvlength);

			// copy serialized pair to writer
			writer.write(memoryWrapper);
		}
	}

	// -------------------------------------------------------------------------
	// Indexed Sorting
	// -------------------------------------------------------------------------

	@Override
	public int compare(int i, int j)
	{
		final byte[] backingArray = this.memory.getBackingArray();
		
		// offsets into index
		final int offsetPositionI = readOffsetPosition(i);
		final int offsetPositionJ = readOffsetPosition(j);
		
		// starts of keys
		final int indexI = readPairOffset(offsetPositionI);
		final int indexJ = readPairOffset(offsetPositionJ);
		
		return comparator.compare(backingArray, backingArray, 
			this.memory.translateOffset(indexI),
			this.memory.translateOffset(indexJ));
	}

	@Override
	public void swap(int i, int j) {
		int offseti = readOffsetPosition(i);
		int offsetj = readOffsetPosition(j);
		writeOffsetPosition(i, offsetj);
		writeOffsetPosition(j, offseti);
	}

	@Override
	public int size()
	{
		return this.pairsCount;
	}

	public final Iterator<PactRecord> getIterator() {

		return new Iterator<PactRecord>() {

			private final PactRecord rec = record;
			private final int size = size();
			private int current = 0;

			@Override
			public boolean hasNext() {
				return this.current < this.size;
			}

			@Override
			public PactRecord next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				
				try {
					getRecord(rec, this.current++);
					return rec;
				}
				catch (IOException ioe) {
					throw new RuntimeException(ioe);
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
