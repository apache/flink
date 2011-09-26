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

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.services.iomanager.BlockChannelReader;
import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;


/**
 * A {@link DataInputView} that is backed by a {@link BlockChannelReader}, making it effectively a data input
 * stream. The view reads it data in blocks from the underlying channel. The view can only read data that
 * has been written by a {@link ChannelWriterOutputView}, due to block formatting.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ChannelReaderInputView implements DataInputView
{
	private final BlockChannelReader reader;
	
	private MemorySegment currentSegment;
	
	private long positionBeforeSegment;
	
	private int positionInSegment;
	
	private int limitInSegment;
	
	private int numRequestsRemaining;
	
	private final int numSegments;
	
	private boolean inLastBlock;
	
	private boolean closed;
	
	private StringBuilder bld;					// reusable stringBuilder for string reading
	
	// --------------------------------------------------------------------------------------------
	
	
	public ChannelReaderInputView(BlockChannelReader reader, List<MemorySegment> memory)
	throws IOException
	{
		this(reader, memory, -1);
	}
	
	public ChannelReaderInputView(BlockChannelReader reader, List<MemorySegment> memory, int numBlocks)
	throws IOException
	{
		if (reader == null || memory == null)
			throw new NullPointerException();
		if (memory.isEmpty())
			throw new IllegalArgumentException("Empty list of memory segments given.");
		if (numBlocks < 1 && numBlocks != -1) {
			throw new IllegalArgumentException("The number of blocks must be a positive number, or -1, if unknown.");
		}
		
		this.reader = reader;
		this.numRequestsRemaining = numBlocks;
		this.numSegments = memory.size();
		
		for (int i = 0; i < memory.size(); i++) {
			sendReadRequest(memory.get(i));
		}
		nextSegment();
	}
	
	/**
	 * Closes this InoutView, closing the underlying reader and returning all memory segments.
	 * 
	 * @return A list containing all memory segments originally supplied to this view.
	 * @throws IOException Thrown, if the underlying reader could not be properly closed.
	 */
	public List<MemorySegment> close() throws IOException
	{	
		if (this.closed) {
			throw new IllegalStateException("Already closed.");
		}
		this.closed = true;
		
		// re-collect all memory segments
		ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(this.numSegments);
		list.add(this.currentSegment);
		this.currentSegment = null;

		// close the writer and gather all segments
		final LinkedBlockingQueue<MemorySegment> queue = this.reader.getReturnQueue();
		this.reader.close();

		for (int i = 1; i < this.numSegments; i++) {
			final MemorySegment m = queue.poll();
			if (m == null) {
				// we get null if the queue is empty. that should not be the case if the reader was properly closed.
				throw new RuntimeException("Bug in ChannelReaderInputView: MemorySegments lost.");
			}
			list.add(m);
		}
		return list;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see java.io.DataInput#readFully(byte[])
	 */
	@Override
	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readFully(byte[], int, int)
	 */
	@Override
	public void readFully(byte[] b, int off, int len) throws IOException
	{
		if (off < 0 | len < 0 | off + len > b.length)
			throw new IndexOutOfBoundsException();
		
		int remaining = this.limitInSegment - this.positionInSegment;
		if (remaining >= len) {
			this.currentSegment.get(this.positionInSegment, b, off, len);
			this.positionInSegment += len;
		}
		else {
			if (remaining == 0) {
				nextSegment();
				remaining = this.limitInSegment - this.positionInSegment;
			}
			
			while (true) {
				int toRead = Math.min(remaining, len);
				this.currentSegment.get(this.positionInSegment, b, off, toRead);
				off += toRead;
				len -= toRead;
				
				if (len > 0) {
					nextSegment();
					remaining = this.limitInSegment - this.positionInSegment;	
				}
				else {
					this.positionInSegment += toRead;
					break;
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#skipBytes(int)
	 */
	@Override
	public int skipBytes(int n) throws IOException
	{
		if (n < 0)
			throw new IllegalArgumentException();
		
		int remaining = this.limitInSegment - this.positionInSegment;
		if (remaining >= n) {
			this.positionInSegment += n;
			return n;
		}
		else {
			if (remaining == 0) {
				try {
					nextSegment();
				}
				catch (EOFException eofex) {
					return 0;
				}
				remaining = this.limitInSegment - this.positionInSegment;
			}
			
			int skipped = 0;
			while (true) {
				int toSkip = Math.min(remaining, n);
				n -= toSkip;
				skipped += toSkip;
				
				if (n > 0) {
					try {
						nextSegment();
					}
					catch (EOFException eofex) {
						return skipped;
					}
					remaining = this.limitInSegment - this.positionInSegment;	
				}
				else {
					this.positionInSegment += toSkip;
					break;
				}
			}
			return skipped;
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readBoolean()
	 */
	@Override
	public boolean readBoolean() throws IOException
	{
		return readByte() == 1;
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readByte()
	 */
	@Override
	public byte readByte() throws IOException
	{
		if (this.positionInSegment < this.limitInSegment) {
			return this.currentSegment.get(this.positionInSegment++);
		}
		else {
			nextSegment();
			return this.currentSegment.get(this.positionInSegment++);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readUnsignedByte()
	 */
	@Override
	public int readUnsignedByte() throws IOException
	{
		return readByte() & 0xff;
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readShort()
	 */
	@Override
	public short readShort() throws IOException
	{
		if (this.positionInSegment < this.limitInSegment - 1) {
			final short v = this.currentSegment.getShort(this.positionInSegment);
			this.positionInSegment += 2;
			return v;
		}
		else if (this.positionInSegment == this.limitInSegment) {
			nextSegment();
			final short v = this.currentSegment.getShort(this.positionInSegment);
			this.positionInSegment += 2;
			return v;
		}
		else {
			return (short) (((readUnsignedByte() << 8) | readUnsignedByte()) & 0xffff);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readUnsignedShort()
	 */
	@Override
	public int readUnsignedShort() throws IOException
	{
		if (this.positionInSegment < this.limitInSegment - 1) {
			final int v = this.currentSegment.getShort(this.positionInSegment) & 0xffff;
			this.positionInSegment += 2;
			return v;
		}
		else if (this.positionInSegment == this.limitInSegment) {
			nextSegment();
			final int v = this.currentSegment.getShort(this.positionInSegment) & 0xffff;
			this.positionInSegment += 2;
			return v;
		}
		else {
			return ((readUnsignedByte() << 8) | readUnsignedByte()) & 0xffff;
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readChar()
	 */
	@Override
	public char readChar() throws IOException 
	{
		if (this.positionInSegment < this.limitInSegment - 1) {
			final char v = this.currentSegment.getChar(this.positionInSegment);
			this.positionInSegment += 2;
			return v;
		}
		else if (this.positionInSegment == this.limitInSegment) {
			nextSegment();
			final char v = this.currentSegment.getChar(this.positionInSegment);
			this.positionInSegment += 2;
			return v;
		}
		else {
			return (char) (((readUnsignedByte() << 8) | readUnsignedByte()) & 0xffff);
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readInt()
	 */
	@Override
	public int readInt() throws IOException
	{
		if (this.positionInSegment < this.limitInSegment - 3) {
			final int v = this.currentSegment.getInt(this.positionInSegment);
			this.positionInSegment += 4;
			return v;
		}
		else if (this.positionInSegment == this.limitInSegment) {
			nextSegment();
			final int v = this.currentSegment.getInt(this.positionInSegment);
			this.positionInSegment += 4;
			return v;
		}
		else {
			return (readUnsignedByte() << 24) |
			       (readUnsignedByte() << 16) |
			       (readUnsignedByte() <<  8) |
			        readUnsignedByte();
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readLong()
	 */
	@Override
	public long readLong() throws IOException
	{
		if (this.positionInSegment < this.limitInSegment - 7) {
			final long v = this.currentSegment.getLong(this.positionInSegment);
			this.positionInSegment += 8;
			return v;
		}
		else if (this.positionInSegment == this.limitInSegment) {
			nextSegment();
			final long v = this.currentSegment.getLong(this.positionInSegment);
			this.positionInSegment += 8;
			return v;
		}
		else {
			long l = 0L;
			l |= (long) (readUnsignedByte() << 56);
			l |= (long) (readUnsignedByte() << 48);
			l |= (long) (readUnsignedByte() << 40);
			l |= (long) (readUnsignedByte() << 32);
			l |= (long) (readUnsignedByte() << 24);
			l |= (long) (readUnsignedByte() << 16);
			l |= (long) (readUnsignedByte() <<  8);
			l |= (long) readUnsignedByte();
			return l;
		}
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readFloat()
	 */
	@Override
	public float readFloat() throws IOException
	{
		return Float.intBitsToFloat(readInt());
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readDouble()
	 */
	@Override
	public double readDouble() throws IOException
	{
		return Double.longBitsToDouble(readLong());
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readLine()
	 */
	@Override
	public String readLine() throws IOException
	{
		if (this.bld == null) {
			this.bld = new StringBuilder();
		}
		final StringBuilder bld = this.bld;
		bld.setLength(0);
		
		try {
			int b;
			while ((b = readUnsignedByte()) != '\n') {
				if (b != '\r')
					bld.append((char) b);
			}
		}
		catch (EOFException eofex) {}

		if (bld.length() == 0)
			return null;
		
		// trim a trailing carriage return
		int len = bld.length();
		if (len > 0 && bld.charAt(len - 1) == '\r') {
			bld.setLength(len - 1);
		}
		return bld.toString();
	}

	/* (non-Javadoc)
	 * @see java.io.DataInput#readUTF()
	 */
	@Override
	public String readUTF() throws IOException
	{
		int utflen = readUnsignedShort();
		byte[] bytearr = new byte[utflen];
		char[] chararr = new char[utflen];

		int c, char2, char3;
		int count = 0;
		int chararr_count = 0;

		readFully(bytearr, 0, utflen);

		while (count < utflen) {
			c = (int) bytearr[count] & 0xff;
			if (c > 127)
				break;
			count++;
			chararr[chararr_count++] = (char) c;
		}

		while (count < utflen) {
			c = (int) bytearr[count] & 0xff;
			switch (c >> 4) {
			case 0:
			case 1:
			case 2:
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
				/* 0xxxxxxx */
				count++;
				chararr[chararr_count++] = (char) c;
				break;
			case 12:
			case 13:
				/* 110x xxxx 10xx xxxx */
				count += 2;
				if (count > utflen)
					throw new UTFDataFormatException("malformed input: partial character at end");
				char2 = (int) bytearr[count - 1];
				if ((char2 & 0xC0) != 0x80)
					throw new UTFDataFormatException("malformed input around byte " + count);
				chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
				break;
			case 14:
				/* 1110 xxxx 10xx xxxx 10xx xxxx */
				count += 3;
				if (count > utflen)
					throw new UTFDataFormatException("malformed input: partial character at end");
				char2 = (int) bytearr[count - 2];
				char3 = (int) bytearr[count - 1];
				if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
					throw new UTFDataFormatException("malformed input around byte " + (count - 1));
				chararr[chararr_count++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
				break;
			default:
				/* 10xx xxxx, 1111 xxxx */
				throw new UTFDataFormatException("malformed input around byte " + count);
			}
		}
		// The number of chars produced may be less than utflen
		return new String(chararr, 0, chararr_count);
	}
	
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataInputView#getPosition()
	 */
	@Override
	public int getPosition() {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataInputView#setPosition(int)
	 */
	@Override
	public DataInputView setPosition(int position)
	{
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataInputView#skip(int)
	 */
	@Override
	public DataInputView skip(int size) throws EOFException
	{
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.services.memorymanager.DataInputView#reset()
	 */
	@Override
	public DataInputView reset()
	{
		throw new UnsupportedOperationException();
	}
	
	// --------------------------------------------------------------------------------------------
	//                                        Utilities
	// --------------------------------------------------------------------------------------------

	private void nextSegment() throws IOException
	{
		// check if we are at our end
		if (this.inLastBlock) {
			throw new EOFException();
		}
				
		// send a request first. if we have only a single segment, this same segment will be the one obtained in
		// the next lines
		if (this.currentSegment != null) {
			this.positionBeforeSegment += this.limitInSegment - ChannelWriterOutputView.HEADER_LENGTH;
			sendReadRequest(this.currentSegment);
		}
		
		// get the next segment
		final MemorySegment seg;
		try {
			seg = this.reader.getReturnQueue().take();
		}
		catch (InterruptedException iex) {
			throw new IOException("ChannelWriterOutputView was interrupted while waiting for the next buffer");
		}
		
		if (seg.getShort(0) != ChannelWriterOutputView.HEADER_MAGIC_NUMBER) {
			throw new IOException("The current block does not belong to a ChannelWriterOutputView / ChannelReaderInputView: Wrong magic number.");
		}
		this.currentSegment = seg;
		this.positionInSegment = ChannelWriterOutputView.HEADER_LENGTH;
		this.limitInSegment = seg.getInt(ChannelWriterOutputView.HEAD_BLOCK_LENGTH_OFFSET);
		
		if ( (seg.getShort(ChannelWriterOutputView.HEADER_FLAGS_OFFSET) & ChannelWriterOutputView.FLAG_LAST_BLOCK) != 0) {
			// last block
			this.numRequestsRemaining = 0;
			this.inLastBlock = true;
		}
	}
	
	private void sendReadRequest(MemorySegment seg) throws IOException
	{
		if (this.numRequestsRemaining != 0) {
			this.reader.readBlock(seg);
			if (this.numRequestsRemaining != -1) {
				this.numRequestsRemaining--;
			}
		} else {
			// directly add it to the end of the return queue
			this.reader.getReturnQueue().add(seg);
		}
	}
}
