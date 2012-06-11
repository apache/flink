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

package eu.stratosphere.nephele.io.channels;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.RecordDeserializer;

/**
 * A class for deserializing a portion of binary data into records of type <code>T</code>. The internal
 * buffer grows dynamically to the size that is required for deserialization.
 * 
 * @author warneke
 * @param <T> The type of the record this deserialization buffer can be used for.
 */
public class DeserializationBuffer<T extends IOReadableWritable> implements RecordDeserializer<T>
{
	/**
	 * The size of an integer in byte.
	 */
	private static final int SIZEOFINT = 4;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * The data input buffer used for deserialization.
	 */
	private final DataInputWrapper deserializationWrapper;

	/**
	 * Buffer to reconstruct the length field.
	 */
	private final ByteBuffer lengthBuf;

	/**
	 * Temporary buffer.
	 */
	private ByteBuffer tempBuffer;
	
	/**
	 * The type of the record to be deserialized.
	 */
	private final Class<? extends T> recordType;
	
	/**
	 * Size of the record to be deserialized in bytes.
	 */
	private int recordLength = -1;

	/**
	 * Flag indicating whether to throw an exception if nothing can be read any more.
	 */
	private final boolean propagateEndOfStream;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Constructs a new deserialization buffer with the specified type.
	 * 
	 * @param recordType The type of the record to be deserialized.
	 */
	public DeserializationBuffer(final Class<? extends T> recordType)
	{
		this(recordType, false);
	}
	
	/**
	 * Constructs a new deserialization buffer with the specified type.
	 * 
	 * @param recordType The type of the record to be deserialized.
	 * @param propagateEndOfStream <code>True</code>, if end of stream notifications during the
	 * 					deserialization process shall be propagated to the caller, <code>false</code> otherwise.
	 */
	public DeserializationBuffer(final Class<? extends T> recordType, final boolean propagateEndOfStream)
	{
		this.recordType = recordType;
		this.propagateEndOfStream = propagateEndOfStream;
		
		this.lengthBuf = ByteBuffer.allocate(SIZEOFINT);
		this.lengthBuf.order(ByteOrder.BIG_ENDIAN);
		
		this.tempBuffer = ByteBuffer.allocate(128);
		this.tempBuffer.order(ByteOrder.BIG_ENDIAN);
		
		this.deserializationWrapper = new DataInputWrapper();
		this.deserializationWrapper.set(this.tempBuffer);
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.RecordDeserializer#readData(java.lang.Object, java.nio.channels.ReadableByteChannel)
	 */
	@Override
	public T readData(T target, final ReadableByteChannel readableByteChannel) throws IOException
	{
		// check whether the length has already been de-serialized
		final int len;
		if (this.recordLength < 0)
		{
			if (readableByteChannel.read(this.lengthBuf) == -1 && this.propagateEndOfStream) {
				if (this.lengthBuf.position() == 0) {
					throw new EOFException();
				} else {
					throw new IOException("Deserialization error: Expected to read " + this.lengthBuf.remaining()
						+ " more bytes of length information from the stream!");
				}
			}

			if (this.lengthBuf.hasRemaining()) {
				return null;
			}

			len = this.lengthBuf.getInt(0);
			this.lengthBuf.clear();

			if (this.tempBuffer.capacity() < len) {
				this.tempBuffer = ByteBuffer.allocate(len);
				this.tempBuffer.order(ByteOrder.BIG_ENDIAN);
				this.deserializationWrapper.set(this.tempBuffer);
			}

			// Important: limit the number of bytes that can be read into the buffer
			this.tempBuffer.position(0);
			this.tempBuffer.limit(len);
		} else {
			len = this.recordLength;
		}

		if (readableByteChannel.read(this.tempBuffer) == -1 && this.propagateEndOfStream) {
			throw new IOException("Deserilization error: Expected to read " + this.tempBuffer.remaining()
				+ " more bytes from stream!");
		}

		if (this.tempBuffer.hasRemaining()) {
			this.recordLength = len;
			return null;
		} else {
			this.recordLength = -1;
		}
		
		this.tempBuffer.position(0);
		this.tempBuffer.limit(len);
		
		if (target == null) {
			target = instantiateTarget();
		}

		// now de-serialize the target
		try {
			target.read(this.deserializationWrapper);
			return target;
		} catch (BufferUnderflowException buex) {
			throw new EOFException();
		}
	}
	
	private final T instantiateTarget() throws IOException
	{
		try {
			return this.recordType.newInstance();
		} catch (Exception e) {
			throw new IOException("Could not instantiate the given record type: " + e.getMessage(), e);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.RecordDeserializer#clear()
	 */
	@Override
	public void clear() {

		this.recordLength = -1;
		if (this.tempBuffer != null) {
			this.tempBuffer.clear();
		}
		if (this.lengthBuf != null) {
			this.lengthBuf.clear();
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.RecordDeserializer#hasUnfinishedData()
	 */
	@Override
	public boolean hasUnfinishedData()
	{
		if (this.recordLength != -1) {
			return true;
		}

		if (this.lengthBuf.position() > 0) {
			return true;
		}

		return false;
	}

	// --------------------------------------------------------------------------------------------
	
	private static final class DataInputWrapper implements DataInput
	{
		private ByteBuffer source;
		
		private byte[] utfByteBuffer;					// reusable byte buffer for utf-8 decoding
		private char[] utfCharBuffer;					// reusable char buffer for utf-8 decoding
		
		
		void set(ByteBuffer source) {
			this.source = source;
		}
		
		
		/* (non-Javadoc)
		 * @see java.io.DataInput#readFully(byte[])
		 */
		@Override
		public void readFully(byte[] b) {
			this.source.get(b);
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readFully(byte[], int, int)
		 */
		@Override
		public void readFully(byte[] b, int off, int len) {
			this.source.get(b, off, len);
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#skipBytes(int)
		 */
		@Override
		public int skipBytes(int n) {
			int newPos = this.source.position() + n;
			if (newPos > this.source.limit()) {
				newPos = this.source.limit();
				n = newPos - this.source.position();
			}
			this.source.position(newPos);
			return n;
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readBoolean()
		 */
		@Override
		public boolean readBoolean() {
			return this.source.get() != 0;
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readByte()
		 */
		@Override
		public byte readByte() {
			return this.source.get();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readUnsignedByte()
		 */
		@Override
		public int readUnsignedByte() {
			return this.source.get() & 0xff;
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readShort()
		 */
		@Override
		public short readShort() {
			return this.source.getShort();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readUnsignedShort()
		 */
		@Override
		public int readUnsignedShort() {
			return this.source.getShort() & 0xffff;
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readChar()
		 */
		@Override
		public char readChar() {
			return this.source.getChar();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readInt()
		 */
		@Override
		public int readInt() {
			return this.source.getInt();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readLong()
		 */
		@Override
		public long readLong() {
			return this.source.getLong();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readFloat()
		 */
		@Override
		public float readFloat() {
			return Float.intBitsToFloat(this.source.getInt());
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readDouble()
		 */
		@Override
		public double readDouble() {
			return Double.longBitsToDouble(this.source.getLong());
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readLine()
		 */
		@Override
		public String readLine()
		{
			if (this.source.hasRemaining()) {
				// read until a newline is found
				StringBuilder bld = new StringBuilder();
				char curr;
				while (this.source.hasRemaining() && (curr = (char) readUnsignedByte()) != '\n') {
					bld.append(curr);
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
	}
}
