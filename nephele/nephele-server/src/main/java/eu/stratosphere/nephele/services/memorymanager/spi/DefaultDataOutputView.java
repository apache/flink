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

public class DefaultDataOutputView extends DefaultMemorySegmentView implements DataOutputView {
	/**
	 * The current write size.
	 */
	private int position;

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	public DefaultDataOutputView(MemorySegmentDescriptor descriptor) {
		super(descriptor);
		position = descriptor.start;
	}

	// -------------------------------------------------------------------------
	// DataOutputView
	// -------------------------------------------------------------------------

	@Override
	public int getPosition() {
		return position - descriptorReference.get().start;
	}

	@Override
	public DataOutputView setPosition(int position) {
		this.position = position + descriptorReference.get().start;
		return this;
	}

	@Override
	public DataOutputView skip(int size) {
		position += size;
		return this;
	}

	@Override
	public DataOutputView reset() {
		position = descriptorReference.get().start;
		return this;
	}

	// ------------------------------------------------------------------------
	// DataOutput
	// ------------------------------------------------------------------------

	@Override
	public void write(int b) throws IOException {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (position < descriptor.end) {
			descriptor.memory[position++] = (byte) (b & 0xff);
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
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (position < descriptor.end && position + len <= descriptor.end && off + len <= b.length) {
			System.arraycopy(b, off, descriptor.memory, position, len);
			position += len;
		} else {
			throw new EOFException();
		}
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (position < descriptor.end) {
			descriptor.memory[position++] = (byte) (v ? 1 : 0);
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
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (position + s.length() < descriptor.end) {
			int length = s.length();
			for (int i = 0; i < length; i++) {
				writeByte(s.charAt(i));
			}
			position += length;
		} else {
			throw new EOFException();
		}
	}

	@Override
	public void writeChar(int v) throws IOException {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (position + 1 < descriptor.end) {
			descriptor.memory[position++] = (byte) ((v >> 8) & 0xff);
			descriptor.memory[position++] = (byte) ((v >> 0) & 0xff);
		} else {
			throw new EOFException();
		}
	}

	@Override
	public void writeChars(String s) throws IOException {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (position + 2 * s.length() < descriptor.end) {
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
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (position + 3 < descriptor.end) {
			descriptor.memory[position++] = (byte) ((v >> 24) & 0xff);
			descriptor.memory[position++] = (byte) ((v >> 16) & 0xff);
			descriptor.memory[position++] = (byte) ((v >> 8) & 0xff);
			descriptor.memory[position++] = (byte) ((v >> 0) & 0xff);
		} else {
			throw new EOFException();
		}
	}

	@Override
	public void writeLong(long v) throws IOException {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (position + 7 < descriptor.end) {
			descriptor.memory[position++] = (byte) ((v >> 56) & 0xff);
			descriptor.memory[position++] = (byte) ((v >> 48) & 0xff);
			descriptor.memory[position++] = (byte) ((v >> 40) & 0xff);
			descriptor.memory[position++] = (byte) ((v >> 32) & 0xff);
			descriptor.memory[position++] = (byte) ((v >> 24) & 0xff);
			descriptor.memory[position++] = (byte) ((v >> 16) & 0xff);
			descriptor.memory[position++] = (byte) ((v >> 8) & 0xff);
			descriptor.memory[position++] = (byte) ((v >> 0) & 0xff);
		} else {
			throw new EOFException();
		}
	}

	@Override
	public void writeShort(int v) throws IOException {
		MemorySegmentDescriptor descriptor = descriptorReference.get();

		if (position + 1 < descriptor.end) {
			descriptor.memory[position++] = (byte) ((v >>> 8) & 0xff);
			descriptor.memory[position++] = (byte) ((v >>> 0) & 0xff);
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
