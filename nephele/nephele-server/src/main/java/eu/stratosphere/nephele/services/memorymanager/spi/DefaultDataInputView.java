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

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager.MemorySegmentDescriptor;

public final class DefaultDataInputView extends DefaultMemorySegmentView implements DataInputView
{
	/**
	 * The current position (in the backing array) to read from.
	 */
	private int position;
	
	/**
	 * The end of the segment in the backing array corresponding to this view.
	 */
	private int end;

	/**
	 * Cached string builder for string assembly.
	 */
	private final StringBuilder bld = new StringBuilder();

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	public DefaultDataInputView(MemorySegmentDescriptor descriptor) {
		super(descriptor);
		this.position = this.offset;
		this.end = descriptor.end;
	}

	// -------------------------------------------------------------------------
	// DataInputView
	// -------------------------------------------------------------------------

	@Override
	public int getPosition() {
		return position - this.offset;
	}

	@Override
	public DataInputView setPosition(int position) {
		this.position = position + this.offset;
		return this;
	}

	@Override
	public DataInputView skip(int size) {
		position += size;
		return this;
	}

	@Override
	public DataInputView reset() {
		position = this.offset;
		return this;
	}

	// ------------------------------------------------------------------------
	// DataInput
	// ------------------------------------------------------------------------

	@Override
	public boolean readBoolean() throws IOException {
		if (position < this.end) {
			return this.memory[position++] != 0;
		} else {
			throw new EOFException();
		}
	}

	@Override
	public byte readByte() throws IOException {
		if (position < this.end) {
			return this.memory[position++];
		} else {
			throw new EOFException();
		}
	}

	@Override
	public char readChar() throws IOException {
		if (position + 1 < this.end) {
			return (char) (((this.memory[position++] & 0xff) << 8) | ((this.memory[position++] & 0xff) << 0));
		} else {
			throw new EOFException();
		}
	}

	@Override
	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}

	@Override
	public float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		if (position < this.end && position + len <= this.end && off + len <= b.length) {
			System.arraycopy(this.memory, position, b, off, len);
			position += len;
		} else {
			throw new EOFException();
		}
	}

	@Override
	public int readInt() throws IOException {
		if (position >= 0 && position + 3 < this.end) {
			return ((this.memory[position++] & 0xff) << 24) | ((this.memory[position++] & 0xff) << 16)
				| ((this.memory[position++] & 0xff) << 8) | ((this.memory[position++] & 0xff) << 0);
		} else {
			throw new EOFException();
		}
	}

	@Override
	public String readLine() throws IOException {
		if (position < this.end) {
			// read until a newline is found
			char curr = readChar();
			while (position < this.end && curr != '\n') {
				bld.append(curr);
				curr = readChar();
			}

			// trim a trailing carriage return
			int len = bld.length();
			if (len > 0 && bld.charAt(len - 1) == '\r') {
				bld.setLength(len - 1);
			}

			String s = bld.toString();
			bld.setLength(0);
			return s;
		} else {
			return null;
		}
	}

	@Override
	public long readLong() throws IOException {
		if (position >= 0 && position + 7 < this.end) {
			return (((long) this.memory[position++] & 0xff) << 56)
				| (((long) this.memory[position++] & 0xff) << 48)
				| (((long) this.memory[position++] & 0xff) << 40)
				| (((long) this.memory[position++] & 0xff) << 32)
				| (((long) this.memory[position++] & 0xff) << 24)
				| (((long) this.memory[position++] & 0xff) << 16)
				| (((long) this.memory[position++] & 0xff) << 8)
				| (((long) this.memory[position++] & 0xff) << 0);
		} else {
			throw new EOFException();
		}
	}

	@Override
	public short readShort() throws IOException {
		if (position >= 0 && position + 1 < this.end) {
			return (short) ((((this.memory[position++]) & 0xff) << 8) | (((this.memory[position++]) & 0xff) << 0));
		} else {
			throw new EOFException();
		}
	}

	@Override
	public String readUTF() throws IOException {
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

	@Override
	public int readUnsignedByte() throws IOException {
		if (position < this.end) {
			return (this.memory[position++] & 0xff);
		} else {
			throw new EOFException();
		}
	}

	@Override
	public int readUnsignedShort() throws IOException {
		if (position + 1 < this.end) {
			return ((this.memory[position++] & 0xff) << 8) | ((this.memory[position++] & 0xff) << 0);
		} else {
			throw new EOFException();
		}
	}

	@Override
	public int skipBytes(int n) throws IOException {
		if (position + n <= this.end) {
			position += n;
			return n;
		} else {
			n = this.end - position;
			position = this.end;
			return n;
		}
	}
}
