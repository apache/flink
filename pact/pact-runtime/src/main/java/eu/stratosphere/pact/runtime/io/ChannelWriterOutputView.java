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

package eu.stratosphere.pact.runtime.io;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;


/**
 * A {@link DataOutputView} that is backed by a {@link BlockChannelWriter}, making it effectively a data output
 * stream. The view writes it data in blocks to the underlying channel, adding a minimal header to each block.
 * The data can be re-read by a {@link ChannelReaderInputView}, if it uses the same block size.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class ChannelWriterOutputView implements DataOutputView
{
	/**
	 * The magic number that identifies blocks as blocks from a ChannelWriterOutputView.
	 */
	protected static final short HEADER_MAGIC_NUMBER = (short) 0xC0FE;
	
	/**
	 * The length of the header put into the blocks.
	 */
	protected static final int HEADER_LENGTH = 8;
	
	/**
	 * The offset to the flags in the header;
	 */
	protected static final int HEADER_FLAGS_OFFSET = 2;
	
	/**
	 * The offset to the header field indicating the number of bytes in the block
	 */
	protected static final int HEAD_BLOCK_LENGTH_OFFSET = 4;
	
	/**
	 * The flag marking a block as the last block.
	 */
	protected static final short FLAG_LAST_BLOCK = (short) 0x1;
	
	// --------------------------------------------------------------------------------------------
	
	private final BlockChannelWriter writer;		// the writer to the channel
	
	private MemorySegment currentSegment;			// the current memory segment to write to
	
	private long positionBeforeSegment;				// the number of bytes written before the current memory segment
	
	private int positionInSegment;					// the offset in the current segment
	
	private int blockCount;							// the number of blocks used
	
	private final int segmentSize;					// the size of the memory segments
	
	private final int numSegments;					// the number of memory segments used by this view
	
	private byte[] utfBuffer;						// the reusable array for UTF encodings
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates an new <code>ChannelWriterOutputView</code> that writes to the given channels and buffers data
	 * in the given memory segments.
	 * 
	 * @param writer The writer to write to.
	 * @param memory The memory used to buffer data.
	 * @param segmentSize The size of the memory segments.
	 */
	public ChannelWriterOutputView(BlockChannelWriter writer, List<MemorySegment> memory, int segmentSize)
	{
		if (writer == null || memory == null)
			throw new NullPointerException();
		if (memory.isEmpty())
			throw new IllegalArgumentException("Empty memory segment collection given.");
		
		this.writer = writer;
		this.segmentSize = segmentSize;
		this.numSegments = memory.size();
		
		// load the segments into the queue
		final LinkedBlockingQueue<MemorySegment> queue = writer.getReturnQueue();
		for (int i = memory.size() - 1; i >= 0; --i) {
			final MemorySegment seg = memory.get(i);
			if (seg.size() != segmentSize) {
				throw new IllegalArgumentException("The supplied memory segments are not of the specified size.");
			}
			queue.add(seg);
		}
		
		// get the first segment
		try {
			nextSegment();
		}
		catch (IOException ioex) {
			throw new RuntimeException("BUG: IOException occurred while getting first block for ChannelWriterOutputView.", ioex);
		}
	}
	
	/**
	 * Closes this OutputView, closing the underlying writer and returning all memory segments.
	 * 
	 * @return A list containing all memory segments originally supplied to this view.
	 * @throws IOException Thrown, if the underlying writer could not be properly closed.
	 */
	public List<MemorySegment> close() throws IOException
	{
		// send off set last segment
		writeCurrentSegment(true);
		this.currentSegment = null;
		
		// close the writer and gather all segments
		final LinkedBlockingQueue<MemorySegment> queue = this.writer.getReturnQueue();
		this.writer.close();
		
		// re-collect all memory segments
		ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(this.numSegments);	
		for (int i = 0; i < this.numSegments; i++) {
			final MemorySegment m = queue.poll();
			if (m == null) {
				// we get null if the queue is empty. that should not be the case if the reader was properly closed.
				throw new RuntimeException("Bug in ChannelWriterOutputView: MemorySegments lost.");
			}
			list.add(m);
		}
		
		return list;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the number of blocks used by this view.
	 * 
	 * @return The number of blocks used.
	 */
	public int getBlockCount()
	{
		return this.blockCount;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see java.io.DataOutput#write(int)
	 */
	@Override
	public void write(int b) throws IOException
	{
		writeByte(b);
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#write(byte[])
	 */
	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#write(byte[], int, int)
	 */
	@Override
	public void write(byte[] b, int off, int len) throws IOException
	{
		int remaining = this.segmentSize - this.positionInSegment;
		if (remaining >= len) {
			this.currentSegment.put(this.positionInSegment, b, off, len);
			this.positionInSegment += len;
		}
		else {
			if (remaining == 0) {
				nextSegment();
				remaining = this.segmentSize - this.positionInSegment;
			}
			while (true) {
				int toPut = Math.min(remaining, len);
				this.currentSegment.put(this.positionInSegment, b, off, toPut);
				off += toPut;
				len -= toPut;
				
				if (len > 0) {
					this.positionInSegment = this.segmentSize;
					nextSegment();
					remaining = this.segmentSize - this.positionInSegment;	
				}
				else {
					this.positionInSegment += toPut;
					break;
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeBoolean(boolean)
	 */
	@Override
	public void writeBoolean(boolean v) throws IOException
	{
		writeByte(v ? 1 : 0);
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeByte(int)
	 */
	@Override
	public void writeByte(int v) throws IOException
	{
		if (this.positionInSegment < this.segmentSize) {
			this.currentSegment.put(this.positionInSegment++, (byte) v);
		}
		else {
			nextSegment();
			this.currentSegment.put(this.positionInSegment++, (byte) v);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeShort(int)
	 */
	@Override
	public void writeShort(int v) throws IOException
	{
		if (this.positionInSegment < this.segmentSize - 1) {
			this.currentSegment.putShort(this.positionInSegment, (short) v);
			this.positionInSegment += 2;
		}
		else if (this.positionInSegment == this.segmentSize) {
			nextSegment();
			this.currentSegment.putShort(this.positionInSegment, (short) v);
			this.positionInSegment += 2;
		}
		else {
			writeByte(v >> 8);
			writeByte(v);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeChar(int)
	 */
	@Override
	public void writeChar(int v) throws IOException
	{
		if (this.positionInSegment < this.segmentSize - 1) {
			this.currentSegment.putChar(this.positionInSegment, (char) v);
			this.positionInSegment += 2;
		}
		else if (this.positionInSegment == this.segmentSize) {
			nextSegment();
			this.currentSegment.putChar(this.positionInSegment, (char) v);
			this.positionInSegment += 2;
		}
		else {
			writeByte(v >> 8);
			writeByte(v);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeInt(int)
	 */
	@Override
	public void writeInt(int v) throws IOException
	{
		if (this.positionInSegment < this.segmentSize - 3) {
			this.currentSegment.putInt(this.positionInSegment, v);
			this.positionInSegment += 4;
		}
		else if (this.positionInSegment == this.segmentSize) {
			nextSegment();
			this.currentSegment.putInt(this.positionInSegment, v);
			this.positionInSegment += 4;
		}
		else {
			writeByte(v >> 24);
			writeByte(v >> 16);
			writeByte(v >>  8);
			writeByte(v);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeLong(long)
	 */
	@Override
	public void writeLong(long v) throws IOException
	{
		if (this.positionInSegment < this.segmentSize - 7) {
			this.currentSegment.putLong(this.positionInSegment, v);
			this.positionInSegment += 8;
		}
		else if (this.positionInSegment == this.segmentSize) {
			nextSegment();
			this.currentSegment.putLong(this.positionInSegment, v);
			this.positionInSegment += 8;
		}
		else {
			writeByte((int) (v >> 56));
			writeByte((int) (v >> 48));
			writeByte((int) (v >> 40));
			writeByte((int) (v >> 32));
			writeByte((int) (v >> 24));
			writeByte((int) (v >> 16));
			writeByte((int) (v >>  8));
			writeByte((int) v);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeFloat(float)
	 */
	@Override
	public void writeFloat(float v) throws IOException
	{
		writeInt(Float.floatToIntBits(v));
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeDouble(double)
	 */
	@Override
	public void writeDouble(double v) throws IOException
	{
		writeLong(Double.doubleToLongBits(v));
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeBytes(java.lang.String)
	 */
	@Override
	public void writeBytes(String s) throws IOException
	{
		for (int i = 0; i < s.length(); i++) {
			writeByte(s.charAt(i));
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeChars(java.lang.String)
	 */
	@Override
	public void writeChars(String s) throws IOException
	{
		for (int i = 0; i < s.length(); i++) {
			writeChar(s.charAt(i));
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataOutput#writeUTF(java.lang.String)
	 */
	@Override
	public void writeUTF(String str) throws IOException
	{
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
			throw new UTFDataFormatException("encoded string too long: " + utflen + " memory");

		if (this.utfBuffer == null || this.utfBuffer.length < utflen + 2) {
			this.utfBuffer = new byte[utflen + 2];
		}
		final byte[] bytearr = this.utfBuffer;

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

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataOutputView#getPosition()
	 */
	@Override
	public int getPosition() {
		if (this.positionBeforeSegment + this.positionInSegment <= Integer.MAX_VALUE) {
			return (int) (this.positionBeforeSegment + this.positionInSegment);
		}
		else {
			throw new RuntimeException("ChannelWriterOutput View exceeded int addressable size - Still incompatible with current memory layout.");
		}
	}
	
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataOutputView#setPosition(int)
	 */
	@Override
	public DataOutput setPosition(int position) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataOutputView#skip(int)
	 */
	@Override
	public DataOutputView skip(int size) throws IOException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataOutputView#reset()
	 */
	@Override
	public DataOutputView reset() {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataOutputView#getRemainingBytes()
	 */
	@Override
	public int getRemainingBytes() {
		// the number of remaining bytes is infinite
		return -1;
	}

	// --------------------------------------------------------------------------------------------
	//                                        Utilities
	// --------------------------------------------------------------------------------------------
	
	private final void nextSegment() throws IOException
	{
		if (this.currentSegment != null) {
			writeCurrentSegment(false);
		}
		
		try {
			this.currentSegment = this.writer.getReturnQueue().take();
		}
		catch (InterruptedException iex) {
			throw new IOException("ChannelWriterOutputView was interrupted while waiting for the next buffer");
		}
		this.positionInSegment = HEADER_LENGTH;
		this.blockCount++;
	}
	
	private final void writeCurrentSegment(boolean lastSegment) throws IOException
	{
		this.currentSegment.putShort(0, HEADER_MAGIC_NUMBER);
		this.currentSegment.putShort(HEADER_FLAGS_OFFSET, lastSegment ? FLAG_LAST_BLOCK : 0);
		this.currentSegment.putInt(HEAD_BLOCK_LENGTH_OFFSET, this.positionInSegment);
		
		this.writer.writeBlock(this.currentSegment);
		this.positionBeforeSegment += this.positionInSegment - HEADER_LENGTH;
	}
}
