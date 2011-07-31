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

import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager.MemorySegmentDescriptor;

public final class DefaultDataOutputView extends DefaultMemorySegmentView implements DataOutputView
{
	/**
	 * The current write size.
	 */
	private int position;
	
	/**
	 * The end of the segment in the backing array corresponding to this view.
	 */
	private int end;

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	public DefaultDataOutputView(MemorySegmentDescriptor descriptor) {
		super(descriptor);
		this.position = this.offset;
		this.end = descriptor.end;
	}
	
	// ------------------------------------------------------------------------------------------------------
	// WARNING: Any code for range checking must take care to avoid integer overflows. The position
	// integer may go up to <code>Integer.MAX_VALUE</tt>. Range checks that work after the principle
	// <code>position + 3 &lt; end</code> may fail because <code>position + 3</code> becomes negative.
	// A safe solution is to subtract the delta from the limit, for example
	// <code>position &lt; end - 3</code>. Since all indices are always positive, and the integer domain
	// has one more negative value than positive values, this can never cause an underflow.
	// ------------------------------------------------------------------------------------------------------

	// -------------------------------------------------------------------------
	// DataOutputView
	// -------------------------------------------------------------------------

	@Override
	public int getPosition() {
		return this.position - this.offset;
	}

	@Override
	public DataOutputView setPosition(int position) {
		final int newPos = this.offset + position;
		if (newPos > this.end) {
			throw new IndexOutOfBoundsException();
		}
		
		this.position = newPos;
		return this;
	}

	@Override
	public DataOutputView skip(int size) throws IOException {
		final int newPos = this.position + size;
		if (newPos > this.end) {
			throw new EOFException();
		}
		this.position = newPos;
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
		if (position < this.end) {
			this.memory[position++] = (byte) (b & 0xff);
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
		if (this.position < this.end && this.position <= this.end - len && off <= b.length - len) {
			System.arraycopy(b, off, this.memory, position, len);
			this.position += len;
		} else {
			throw new EOFException();
		}
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		if (this.position < this.end) {
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
		final int sLen = s.length();
		
		if (this.position < this.end - sLen) {
			for (int i = 0; i < sLen; i++) {
				writeByte(s.charAt(i));
			}
		} else {
			throw new EOFException();
		}
	}

	@Override
	public void writeChar(int v) throws IOException {
		if (position < this.end - 1) {
			this.memory[position++] = (byte) ((v >> 8) & 0xff);
			this.memory[position++] = (byte) ((v >> 0) & 0xff);
		} else {
			throw new EOFException();
		}
	}

	@Override
	public void writeChars(String s) throws IOException {
		if (position < this.end - (2 * s.length())) {
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
		if (position < this.end - 3) {
			this.memory[position++] = (byte) ((v >> 24) & 0xff);
			this.memory[position++] = (byte) ((v >> 16) & 0xff);
			this.memory[position++] = (byte) ((v >> 8) & 0xff);
			this.memory[position++] = (byte) ((v >> 0) & 0xff);
		} else {
			throw new EOFException();
		}
	}

	@Override
	public void writeLong(long v) throws IOException {
		if (position < this.end - 7) {
			this.memory[position++] = (byte) ((v >> 56) & 0xff);
			this.memory[position++] = (byte) ((v >> 48) & 0xff);
			this.memory[position++] = (byte) ((v >> 40) & 0xff);
			this.memory[position++] = (byte) ((v >> 32) & 0xff);
			this.memory[position++] = (byte) ((v >> 24) & 0xff);
			this.memory[position++] = (byte) ((v >> 16) & 0xff);
			this.memory[position++] = (byte) ((v >> 8) & 0xff);
			this.memory[position++] = (byte) ((v >> 0) & 0xff);
		} else {
			throw new EOFException();
		}
	}

	@Override
	public void writeShort(int v) throws IOException {
		if (position < this.end - 1) {
			this.memory[position++] = (byte) ((v >>> 8) & 0xff);
			this.memory[position++] = (byte) ((v >>> 0) & 0xff);
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
			throw new UTFDataFormatException("encoded string too long: " + utflen + " memory");

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
