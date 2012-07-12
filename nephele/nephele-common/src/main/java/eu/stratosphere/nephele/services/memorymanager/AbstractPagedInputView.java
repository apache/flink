/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;


/**
 * The base class for all input views that are backed by multiple memory pages. This base class contains all
 * decoding methods to read data from a page and detect page boundary crossing. The concrete sub classes must
 * implement the methods to provide the next memory page once the boundary is crossed.
 *
 * @author Stephan Ewen
 */
public abstract class AbstractPagedInputView implements DataInputView
{
	private MemorySegment currentSegment;
	
	protected final int headerLength;				// the number of bytes to skip at the beginning of each segment
	
	private int positionInSegment;					// the offset in the current segment
	
	private int limitInSegment;						// the limit in the current segment before switching to the next
	
	private byte[] utfByteBuffer;					// reusable byte buffer for utf-8 decoding
	private char[] utfCharBuffer;					// reusable char buffer for utf-8 decoding
	
	
	// --------------------------------------------------------------------------------------------
	//                                    Constructors
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new view that starts with the given segment. The input starts directly after the header
	 * of the given page. If the header size is zero, it starts at the beginning. The specified initial
	 * limit describes up to which position data may be read from the current segment, before the view must
	 * advance to the next segment.
	 * 
	 * @param initialSegment The memory segment to start reading from.
	 * @param initialLimit The position one after the last valid byte in the initial segment.
	 * @param headerLength The number of bytes to skip at the beginning of each segment for the header. This
	 *                     length must be the same for all memory segments.
	 */
	protected AbstractPagedInputView(MemorySegment initialSegment, int initialLimit, int headerLength)
	{
		this.headerLength = headerLength;
		this.positionInSegment = headerLength;
		seekInput(initialSegment, headerLength, initialLimit);
	}
	
	/**
	 * Creates a new view that is initially not bound to a memory segment. This constructor is typically
	 * for views that always seek first.
	 * <p>
	 * WARNING: The view is not readable until the first call to either {@link #advance()}, 
	 * or to {@link #seekInput(MemorySegment, int, int)}.
	 * 
	 * @param headerLength The number of bytes to skip at the beginning of each segment for the header.
	 */
	protected AbstractPagedInputView(int headerLength)
	{
		this.headerLength = headerLength;
	}

	// --------------------------------------------------------------------------------------------
	//                                  Page Management
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the memory segment that will be used to read the next bytes from. If the segment is exactly exhausted,
	 * meaning that the last byte read was the last byte available in the segment, then this segment will
	 * not serve the next bytes. The segment to serve the next bytes will be obtained through the
	 * {@link #nextSegment(MemorySegment)} method.
	 * 
	 * @return The current memory segment.
	 */
	public MemorySegment getCurrentSegment() {
		return this.currentSegment;
	}
	
	/**
	 * Gets the position from which the next byte will be read. If that position is equal to the current limit,
	 * then the next byte will be read from next segment.
	 * 
	 * @return
	 * @see #getCurrentSegmentLimit()
	 */
	public int getCurrentPositionInSegment() {
		return this.positionInSegment;
	}
	
	/**
	 * Gets the current limit in the memory segment. This value points to the byte one after the last valid byte
	 * in the memory segment.
	 * 
	 * @return The current limit in the memory segment.
	 * @see #getCurrentPositionInSegment()
	 */
	public int getCurrentSegmentLimit() {
		return this.limitInSegment;
	}
	
	/**
	 * The method by which concrete subclasses realize page crossing. This method is invoked when the current page
	 * is exhausted and a new page is required to continue the reading. If no further page is available, this
	 * method must throw an {@link EOFException}.
	 *  
	 * @param current The current page that was read to its limit. May be {@code null}, if this method is
	 *                invoked for the first time.
	 * @return The next page from which the reading should continue. May not be {@code null}. If the input is
	 *         exhausted, an {@link EOFException} must be thrown instead.
	 *         
	 * @throws EOFException Thrown, if no further segment is available.
	 * @throws IOException Thrown, if the method cannot provide the next page due to an I/O related problem.
	 */
	protected abstract MemorySegment nextSegment(MemorySegment current) throws EOFException, IOException;
	
	/**
	 * Gets the limit for reading bytes from the given memory segment. This method must return the position
	 * of the byte after the last valid byte in the given memory segment. When the position returned by this
	 * method is reached, the view will attempt to switch to the next memory segment.
	 * 
	 * @param segment The segment to determine the limit for.
	 * @return The limit for the given memory segment.
	 */
	protected abstract int getLimitForSegment(MemorySegment segment);
	
	/**
	 * Advances the view to the next memory segment. The reading will continue after the header of the next
	 * segment. This method uses {@link #nextSegment(MemorySegment)} and {@link #getLimitForSegment(MemorySegment)}
	 * to get the next segment and set its limit.
	 * 
	 * @throws IOException Thrown, if the next segment could not be obtained.
	 * 
	 * @see #nextSegment(MemorySegment)
	 * @see #getLimitForSegment(MemorySegment)
	 */
	protected final void advance() throws IOException
	{
		// note: this code ensures that in case of EOF, we stay at the same position such that
		// EOF is reproducible (if nextSegment throws a reproducible EOFException)
		this.currentSegment = nextSegment(this.currentSegment);
		this.limitInSegment = getLimitForSegment(this.currentSegment);
		this.positionInSegment = this.headerLength;
	}
	
	/**
	 * Sets the internal state of the view such that the next bytes will be read from the given memory segment,
	 * starting at the given position. The memory segment will provide bytes up to the given limit position.
	 * 
	 * @param segment The segment to read the next bytes from.
	 * @param positionInSegment The position in the segment to start reading from.
	 * @param limitInSegment The limit in the segment. When reached, the view will attempt to switch to
	 *                       the next segment.
	 */
	protected void seekInput(MemorySegment segment, int positionInSegment, int limitInSegment)
	{
		this.currentSegment = segment;
		this.positionInSegment = positionInSegment;
		this.limitInSegment = limitInSegment;
	}
	
	/**
	 * Clears the internal state of the view. After this call, all read attempts will fail, until the
	 * {@link #advance()} or {@link #seekInput(MemorySegment, int, int)} method have been invoked.
	 */
	protected void clear()
	{
		this.currentSegment = null;
		this.positionInSegment = this.headerLength;
		this.limitInSegment = headerLength;
	}
	
	// --------------------------------------------------------------------------------------------
	//                               Data Input Specific methods
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
		if (off < 0 || len < 0 || off + len > b.length)
			throw new IndexOutOfBoundsException();
		
		int remaining = this.limitInSegment - this.positionInSegment;
		if (remaining >= len) {
			this.currentSegment.get(this.positionInSegment, b, off, len);
			this.positionInSegment += len;
		}
		else {
			if (remaining == 0) {
				advance();
				remaining = this.limitInSegment - this.positionInSegment;
			}
			
			while (true) {
				int toRead = Math.min(remaining, len);
				this.currentSegment.get(this.positionInSegment, b, off, toRead);
				off += toRead;
				len -= toRead;
				
				if (len > 0) {
					advance();
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
			advance();
			return readByte();
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
			advance();
			return readShort();
		}
		else {
			return (short) ((readUnsignedByte() << 8) | readUnsignedByte());
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
			advance();
			return readUnsignedShort();
		}
		else {
			return (readUnsignedByte() << 8) | readUnsignedByte();
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
			advance();
			return readChar();
		}
		else {
			return (char) ((readUnsignedByte() << 8) | readUnsignedByte());
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
			advance();
			return readInt();
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
			advance();
			return readLong();
		}
		else {
			long l = 0L;
			l |= ((long) readUnsignedByte()) << 56;
			l |= ((long) readUnsignedByte()) << 48;
			l |= ((long) readUnsignedByte()) << 40;
			l |= ((long) readUnsignedByte()) << 32;
			l |= ((long) readUnsignedByte()) << 24;
			l |= ((long) readUnsignedByte()) << 16;
			l |= ((long) readUnsignedByte()) <<  8;
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
		final StringBuilder bld = new StringBuilder(32);
		
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
		final int utflen = readUnsignedShort();
		
		final byte[] bytearr;
		final char[] chararr;
		
		if (this.utfByteBuffer == null || this.utfByteBuffer.length < utflen) {
			bytearr = new byte[utflen];
			this.utfByteBuffer = bytearr;
		} else {
			bytearr = this.utfByteBuffer;
		}
		if (this.utfCharBuffer == null || this.utfCharBuffer.length < utflen) {
			chararr = new char[utflen];
			this.utfCharBuffer = chararr;
		} else {
			chararr = this.utfCharBuffer;
		}

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
					advance();
				} catch (EOFException eofex) {
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
						advance();
					} catch (EOFException eofex) {
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
	 * @see eu.stratosphere.nephele.services.memorymanager.DataInputView#skipBytesToRead(int)
	 */
	@Override
	public void skipBytesToRead(int numBytes) throws IOException
	{
		if (numBytes < 0)
			throw new IllegalArgumentException();
		
		int remaining = this.limitInSegment - this.positionInSegment;
		if (remaining >= numBytes) {
			this.positionInSegment += numBytes;
		}
		else {
			if (remaining == 0) {
				advance();
				remaining = this.limitInSegment - this.positionInSegment;
			}
			
			while (true) {
				if (numBytes > remaining) {
					numBytes -= remaining;
					advance();
					remaining = this.limitInSegment - this.positionInSegment;	
				}
				else {
					this.positionInSegment += numBytes;
					break;
				}
			}
		}
	}
}
