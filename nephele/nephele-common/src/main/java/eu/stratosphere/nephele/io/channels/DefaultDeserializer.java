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

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.services.memorymanager.DataInputView;

/**
 * A class for deserializing a portion of binary data into records of type <code>T</code>. The internal
 * buffer grows dynamically to the size that is required for deserialization.
 * 
 * @author warneke
 * @param <T> The type of the record this deserialization buffer can be used for.
 */
public class DefaultDeserializer<T extends IOReadableWritable> implements RecordDeserializer<T>
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
	public DefaultDeserializer(final Class<? extends T> recordType)
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
	public DefaultDeserializer(final Class<? extends T> recordType, final boolean propagateEndOfStream)
	{
		this.recordType = recordType;
		this.propagateEndOfStream = propagateEndOfStream;
		
		this.lengthBuf = ByteBuffer.allocate(SIZEOFINT);
		this.lengthBuf.order(ByteOrder.BIG_ENDIAN);
		
		this.tempBuffer = ByteBuffer.allocate(128);
		this.tempBuffer.order(ByteOrder.BIG_ENDIAN);
		
		this.deserializationWrapper = new DataInputWrapper();
		this.deserializationWrapper.setArray(this.tempBuffer.array());
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
				this.deserializationWrapper.setArray(this.tempBuffer.array());
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
		
		this.deserializationWrapper.reset(len);
		
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
	
//	private static final class DataInputWrapper implements DataInputView
//	{
//		private ByteBuffer source;
//		
//		private byte[] utfByteBuffer;					// reusable byte buffer for utf-8 decoding
//		private char[] utfCharBuffer;					// reusable char buffer for utf-8 decoding
//		
//		
//		void set(ByteBuffer source) {
//			this.source = source;
//		}
//		
//		
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readFully(byte[])
//		 */
//		@Override
//		public void readFully(byte[] b) {
//			this.source.get(b);
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readFully(byte[], int, int)
//		 */
//		@Override
//		public void readFully(byte[] b, int off, int len) {
//			this.source.get(b, off, len);
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#skipBytes(int)
//		 */
//		@Override
//		public int skipBytes(int n) {
//			int newPos = this.source.position() + n;
//			if (newPos > this.source.limit()) {
//				newPos = this.source.limit();
//				n = newPos - this.source.position();
//			}
//			this.source.position(newPos);
//			return n;
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readBoolean()
//		 */
//		@Override
//		public boolean readBoolean() {
//			return this.source.get() != 0;
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readByte()
//		 */
//		@Override
//		public byte readByte() {
//			return this.source.get();
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readUnsignedByte()
//		 */
//		@Override
//		public int readUnsignedByte() {
//			return this.source.get() & 0xff;
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readShort()
//		 */
//		@Override
//		public short readShort() {
//			return this.source.getShort();
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readUnsignedShort()
//		 */
//		@Override
//		public int readUnsignedShort() {
//			return this.source.getShort() & 0xffff;
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readChar()
//		 */
//		@Override
//		public char readChar() {
//			return this.source.getChar();
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readInt()
//		 */
//		@Override
//		public int readInt() {
//			return this.source.getInt();
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readLong()
//		 */
//		@Override
//		public long readLong() {
//			return this.source.getLong();
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readFloat()
//		 */
//		@Override
//		public float readFloat() {
//			return Float.intBitsToFloat(this.source.getInt());
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readDouble()
//		 */
//		@Override
//		public double readDouble() {
//			return Double.longBitsToDouble(this.source.getLong());
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readLine()
//		 */
//		@Override
//		public String readLine()
//		{
//			if (this.source.hasRemaining()) {
//				// read until a newline is found
//				StringBuilder bld = new StringBuilder();
//				char curr;
//				while (this.source.hasRemaining() && (curr = (char) readUnsignedByte()) != '\n') {
//					bld.append(curr);
//				}
//				// trim a trailing carriage return
//				int len = bld.length();
//				if (len > 0 && bld.charAt(len - 1) == '\r') {
//					bld.setLength(len - 1);
//				}
//				String s = bld.toString();
//				bld.setLength(0);
//				return s;
//			} else {
//				return null;
//			}
//		}
//
//		/* (non-Javadoc)
//		 * @see java.io.DataInput#readUTF()
//		 */
//		@Override
//		public String readUTF() throws IOException
//		{
//			final int utflen = readUnsignedShort();
//			
//			final byte[] bytearr;
//			final char[] chararr;
//			
//			if (this.utfByteBuffer == null || this.utfByteBuffer.length < utflen) {
//				bytearr = new byte[utflen];
//				this.utfByteBuffer = bytearr;
//			} else {
//				bytearr = this.utfByteBuffer;
//			}
//			if (this.utfCharBuffer == null || this.utfCharBuffer.length < utflen) {
//				chararr = new char[utflen];
//				this.utfCharBuffer = chararr;
//			} else {
//				chararr = this.utfCharBuffer;
//			}
//
//			int c, char2, char3;
//			int count = 0;
//			int chararr_count = 0;
//
//			readFully(bytearr, 0, utflen);
//
//			while (count < utflen) {
//				c = (int) bytearr[count] & 0xff;
//				if (c > 127)
//					break;
//				count++;
//				chararr[chararr_count++] = (char) c;
//			}
//
//			while (count < utflen) {
//				c = (int) bytearr[count] & 0xff;
//				switch (c >> 4) {
//				case 0:
//				case 1:
//				case 2:
//				case 3:
//				case 4:
//				case 5:
//				case 6:
//				case 7:
//					/* 0xxxxxxx */
//					count++;
//					chararr[chararr_count++] = (char) c;
//					break;
//				case 12:
//				case 13:
//					/* 110x xxxx 10xx xxxx */
//					count += 2;
//					if (count > utflen)
//						throw new UTFDataFormatException("malformed input: partial character at end");
//					char2 = (int) bytearr[count - 1];
//					if ((char2 & 0xC0) != 0x80)
//						throw new UTFDataFormatException("malformed input around byte " + count);
//					chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
//					break;
//				case 14:
//					/* 1110 xxxx 10xx xxxx 10xx xxxx */
//					count += 3;
//					if (count > utflen)
//						throw new UTFDataFormatException("malformed input: partial character at end");
//					char2 = (int) bytearr[count - 2];
//					char3 = (int) bytearr[count - 1];
//					if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
//						throw new UTFDataFormatException("malformed input around byte " + (count - 1));
//					chararr[chararr_count++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
//					break;
//				default:
//					/* 10xx xxxx, 1111 xxxx */
//					throw new UTFDataFormatException("malformed input around byte " + count);
//				}
//			}
//			// The number of chars produced may be less than utflen
//			return new String(chararr, 0, chararr_count);
//		}
//
//
//		/* (non-Javadoc)
//		 * @see eu.stratosphere.nephele.services.memorymanager.DataInputView#skipBytesToRead(int)
//		 */
//		@Override
//		public void skipBytesToRead(int numBytes) throws IOException {
//			if (this.source.remaining() < numBytes) {
//				throw new EOFException();
//			} else {
//				this.source.position(this.source.position() + numBytes);
//			}
//		}
//	}
	
	private static final class DataInputWrapper implements DataInputView
	{
		private byte[] source;
		
		private int position;
		private int limit;
		
		private char[] utfCharBuffer;					// reusable char buffer for utf-8 decoding
		
		
		void setArray(byte[] source) {
			this.source = source;
		}
		
		void reset(int limit) {
			this.position = 0;
			this.limit = limit;
		}
		
		
		/* (non-Javadoc)
		 * @see java.io.DataInput#readFully(byte[])
		 */
		@Override
		public void readFully(byte[] b) throws EOFException {
			readFully(b, 0, b.length);
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readFully(byte[], int, int)
		 */
		@Override
		public void readFully(byte[] b, int off, int len) throws EOFException {
			if (this.position <= this.limit - len) {
				System.arraycopy(this.source, this.position, b, off, len);
			} else {
				throw new EOFException();
			}
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#skipBytes(int)
		 */
		@Override
		public int skipBytes(int n) {
			if (n < 0) {
				throw new IllegalArgumentException("Number of bytes to skip must not be negative.");
			}
			
			int toSkip = Math.min(this.limit - this.position, n);
			this.position += toSkip;
			return toSkip;
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readBoolean()
		 */
		@Override
		public boolean readBoolean() throws EOFException{
			return readByte() != 0;
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readByte()
		 */
		@Override
		public byte readByte() throws EOFException {
			if (this.position < this.limit) {
				return this.source[this.position++];
			} else {
				throw new EOFException();
			}
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readUnsignedByte()
		 */
		@Override
		public int readUnsignedByte() throws EOFException {
			return readByte() & 0xff;
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readShort()
		 */
		@Override
		public short readShort() throws EOFException {
			if (this.position < this.limit - 1) {
				short num = (short) (
						((this.source[this.position + 0] & 0xff) << 8) |
						((this.source[this.position + 1] & 0xff)     ) );
				this.position += 2;
				return num;
			} else {
				throw new EOFException();
			}
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readUnsignedShort()
		 */
		@Override
		public int readUnsignedShort() throws EOFException {
			return readShort() & 0xffff;
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readChar()
		 */
		@Override
		public char readChar() throws EOFException {
			if (this.position < this.limit - 1) {
				char c = (char) (
						((this.source[this.position + 0] & 0xff) << 8) |
						((this.source[this.position + 1] & 0xff)     ) );
				this.position += 2;
				return c;
			} else {
				throw new EOFException();
			}
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readInt()
		 */
		@Override
		public int readInt() throws EOFException {
			if (this.position < this.limit - 3) {
				final int num = ((this.source[this.position + 0] & 0xff) << 24) |
								((this.source[this.position + 1] & 0xff) << 16) |
								((this.source[this.position + 2] & 0xff) <<  8) |
								((this.source[this.position + 3] & 0xff) );
				this.position += 4;
				return num;
			} else {
				throw new EOFException();
			}
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readLong()
		 */
		@Override
		public long readLong() throws EOFException {
			if (this.position < this.limit - 7) {
				final long num = (((long) this.source[this.position + 0] & 0xff) << 56) |
								 (((long) this.source[this.position + 1] & 0xff) << 48) |
								 (((long) this.source[this.position + 2] & 0xff) << 40) |
								 (((long) this.source[this.position + 3] & 0xff) << 32) |
								 (((long) this.source[this.position + 4] & 0xff) << 24) |
								 (((long) this.source[this.position + 5] & 0xff) << 16) |
								 (((long) this.source[this.position + 6] & 0xff) <<  8) |
								 (((long) this.source[this.position + 7] & 0xff) <<  0);
				this.position += 8;
				return num;
			} else {
				throw new EOFException();
			}
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readFloat()
		 */
		@Override
		public float readFloat() throws EOFException {
			return Float.intBitsToFloat(readInt());
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readDouble()
		 */
		@Override
		public double readDouble() throws EOFException {
			return Double.longBitsToDouble(readLong());
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readLine()
		 */
		@Override
		public String readLine()
		{
			if (this.position < this.limit) {
				// read until a newline is found
				StringBuilder bld = new StringBuilder();
				char curr;
				while (this.position < this.limit && (curr = (char) (this.source[this.position++] & 0xff)) != '\n') {
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
			final int utfLimit = this.position + utflen;
			
			if (utfLimit > this.limit) {
				throw new EOFException();
			}
			
			final byte[] bytearr = this.source;
			final char[] chararr;
			if (this.utfCharBuffer == null || this.utfCharBuffer.length < utflen) {
				chararr = new char[utflen];
				this.utfCharBuffer = chararr;
			} else {
				chararr = this.utfCharBuffer;
			}

			
			int c, char2, char3;
			int count = this.position;
			int chararr_count = 0;

			while (count < utfLimit) {
				c = (int) bytearr[count] & 0xff;
				if (c > 127)
					break;
				count++;
				chararr[chararr_count++] = (char) c;
			}

			while (count < utfLimit) {
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
					if (count > utfLimit)
						throw new UTFDataFormatException("Malformed input: partial character at end");
					char2 = (int) bytearr[count - 1];
					if ((char2 & 0xC0) != 0x80)
						throw new UTFDataFormatException("Malformed input around byte " + count);
					chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
					break;
				case 14:
					/* 1110 xxxx 10xx xxxx 10xx xxxx */
					count += 3;
					if (count > utfLimit)
						throw new UTFDataFormatException("Malformed input: partial character at end");
					char2 = (int) bytearr[count - 2];
					char3 = (int) bytearr[count - 1];
					if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
						throw new UTFDataFormatException("Malformed input around byte " + (count - 1));
					chararr[chararr_count++] = (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
					break;
				default:
					/* 10xx xxxx, 1111 xxxx */
					throw new UTFDataFormatException("Malformed input around byte " + count);
				}
			}
			// The number of chars produced may be less than utflen
			this.position += utflen;
			return new String(chararr, 0, chararr_count);
		}


		/* (non-Javadoc)
		 * @see eu.stratosphere.nephele.services.memorymanager.DataInputView#skipBytesToRead(int)
		 */
		@Override
		public void skipBytesToRead(int numBytes) throws EOFException
		{
			if (numBytes < 0) {
				throw new IllegalArgumentException("Number of bytes to skip must not be negative.");
			} else if (this.limit - this.position < numBytes) {
				throw new EOFException();
			} else {
				this.position += numBytes;
			}
		}
	}
}
