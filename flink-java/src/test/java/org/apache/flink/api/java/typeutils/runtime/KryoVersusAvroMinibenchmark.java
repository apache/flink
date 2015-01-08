/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.typeutils.runtime;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemoryUtils;

public class KryoVersusAvroMinibenchmark {

	private static final long SEED = 94762389741692387L;
	
	private static final Random rnd = new Random(SEED);
	
	private static final int NUM_ELEMENTS = 100000;
	
	private static final int NUM_RUNS = 10;
	
	
	
	public static void main(String[] args) throws Exception {
		
		final MyType[] elements = new MyType[NUM_ELEMENTS];
		for (int i = 0; i < NUM_ELEMENTS; i++) {
			elements[i] = MyType.getRandom();
		}
		
		final MyType dummy = new MyType();
		
		long[] timesAvro = new long[NUM_RUNS];
		long[] timesKryo = new long[NUM_RUNS];
		
		for (int i = 0; i < NUM_RUNS; i++) {
			System.out.println("----------------- Starting run " + i + " ---------------------");
			
			System.out.println("Avro serializer");
			{
				final TestDataOutputSerializer outView = new TestDataOutputSerializer(100000000);
				final AvroSerializer<MyType> serializer = new AvroSerializer<MyType>(MyType.class);
				
				long start = System.nanoTime();
				
				for (int k = 0; k < NUM_ELEMENTS; k++) {
					serializer.serialize(elements[k], outView);
				}
				
				final DataInputDeserializer inView = new DataInputDeserializer(outView.wrapAsByteBuffer());
				for (int k = 0; k < NUM_ELEMENTS; k++) {
					serializer.deserialize(dummy, inView);
				}
				
				long elapsed = System.nanoTime() - start;
				System.out.println("Took: " + (elapsed / 1000000) + " msecs");
				timesAvro[i] = elapsed;
			}
			
			System.gc();
			
			System.out.println("Kryo serializer");
			{
				final TestDataOutputSerializer outView = new TestDataOutputSerializer(100000000);
				final KryoSerializer<MyType> serializer = new KryoSerializer<MyType>(MyType.class);
				serializer.getKryo().register(Tuple2.class);
				
				long start = System.nanoTime();
				
				for (int k = 0; k < NUM_ELEMENTS; k++) {
					serializer.serialize(elements[k], outView);
				}
				
				final DataInputDeserializer inView = new DataInputDeserializer(outView.wrapAsByteBuffer());
				for (int k = 0; k < NUM_ELEMENTS; k++) {
					serializer.deserialize(dummy, inView);
				}
				
				long elapsed = System.nanoTime() - start;
				System.out.println("Took: " + (elapsed / 1000000) + " msecs");
				timesKryo[i] = elapsed;
			}
		}
	}
	
	
	
	
	
	public static class MyType {
		
		private String theString;
		
//		private Tuple2<Long, Double> theTuple;
		
		private List<Integer> theList;

		
		public MyType() {
			theString = "";
//			theTuple = new Tuple2<Long, Double>(0L, 0.0);
			theList = new ArrayList<Integer>();
		}
		
		public MyType(String theString, Tuple2<Long, Double> theTuple, List<Integer> theList) {
			this.theString = theString;
//			this.theTuple = theTuple;
			this.theList = theList;
		}

		
		public String getTheString() {
			return theString;
		}

		public void setTheString(String theString) {
			this.theString = theString;
		}

//		public Tuple2<Long, Double> getTheTuple() {
//			return theTuple;
//		}
//
//		public void setTheTuple(Tuple2<Long, Double> theTuple) {
//			this.theTuple = theTuple;
//		}

		public List<Integer> getTheList() {
			return theList;
		}

		public void setTheList(List<Integer> theList) {
			this.theList = theList;
		}
		
		
		public static MyType getRandom() {
			final int numListElements = rnd.nextInt(20);
			List<Integer> list = new ArrayList<Integer>(numListElements);
			for (int i = 0; i < numListElements; i++) {
				list.add(rnd.nextInt());
			}
			
			return new MyType(randomString(), new Tuple2<Long, Double>(rnd.nextLong(), rnd.nextDouble()), list);
		}
	}
	
	
	private static String randomString() {
		final int len = rnd.nextInt(100) + 20;
		
		StringBuilder bld = new StringBuilder();
		for (int i = 0; i < len; i++) {
			bld.append(rnd.nextInt('z' - 'a' + 1) + 'a');
		}
		return bld.toString();
	}
	
	// ============================================================================================
	// ============================================================================================
	
	public static final class DataInputDeserializer implements DataInputView {
		
		private byte[] buffer;
		
		private int end;

		private int position;

		public DataInputDeserializer() {
		}
		
		public DataInputDeserializer(byte[] buffer, int start, int len) {
			setBuffer(buffer, start, len);
		}
		
		public DataInputDeserializer(ByteBuffer buffer) {
			setBuffer(buffer);
		}

		public void setBuffer(ByteBuffer buffer) {
			if (buffer.hasArray()) {
				this.buffer = buffer.array();
				this.position = buffer.arrayOffset() + buffer.position();
				this.end = this.position + buffer.remaining();
			} else if (buffer.isDirect()) {
				this.buffer = new byte[buffer.remaining()];
				this.position = 0;
				this.end = this.buffer.length;

				buffer.get(this.buffer);
			} else {
				throw new IllegalArgumentException("The given buffer is neither an array-backed heap ByteBuffer, nor a direct ByteBuffer.");
			}
		}

		public void setBuffer(byte[] buffer, int start, int len) {
			if (buffer == null) {
				throw new NullPointerException();
			}

			if (start < 0 || len < 0 || start + len >= buffer.length) {
				throw new IllegalArgumentException();
			}

			this.buffer = buffer;
			this.position = start;
			this.end = start * len;
		}

		// ----------------------------------------------------------------------------------------
		//                               Data Input
		// ----------------------------------------------------------------------------------------
		
		@Override
		public boolean readBoolean() throws IOException {
			if (this.position < this.end) {
				return this.buffer[this.position++] != 0;
			} else {
				throw new EOFException();
			}
		}

		@Override
		public byte readByte() throws IOException {
			if (this.position < this.end) {
				return this.buffer[this.position++];
			} else {
				throw new EOFException();
			}
		}

		@Override
		public char readChar() throws IOException {
			if (this.position < this.end - 1) {
				return (char) (((this.buffer[this.position++] & 0xff) << 8) | ((this.buffer[this.position++] & 0xff) << 0));
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
			if (len >= 0) {
				if (off <= b.length - len) {
					if (this.position <= this.end - len) {
						System.arraycopy(this.buffer, position, b, off, len);
						position += len;
					} else {
						throw new EOFException();
					}
				} else {
					throw new ArrayIndexOutOfBoundsException();
				}
			} else if (len < 0) {
				throw new IllegalArgumentException("Length may not be negative.");
			}
		}

		@Override
		public int readInt() throws IOException {
			if (this.position >= 0 && this.position < this.end - 3) {
				@SuppressWarnings("restriction")
				int value = UNSAFE.getInt(this.buffer, BASE_OFFSET + this.position);
				if (LITTLE_ENDIAN) {
					value = Integer.reverseBytes(value);
				}
				
				this.position += 4;
				return value;
			} else {
				throw new EOFException();
			}
		}

		@Override
		public String readLine() throws IOException {
			if (this.position < this.end) {
				// read until a newline is found
				StringBuilder bld = new StringBuilder();
				char curr = (char) readUnsignedByte();
				while (position < this.end && curr != '\n') {
					bld.append(curr);
					curr = (char) readUnsignedByte();
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
			if (position >= 0 && position < this.end - 7) {
				@SuppressWarnings("restriction")
				long value = UNSAFE.getLong(this.buffer, BASE_OFFSET + this.position);
				if (LITTLE_ENDIAN) {
					value = Long.reverseBytes(value);
				}
				this.position += 8;
				return value;
			} else {
				throw new EOFException();
			}
		}

		@Override
		public short readShort() throws IOException {
			if (position >= 0 && position < this.end - 1) {
				return (short) ((((this.buffer[position++]) & 0xff) << 8) | (((this.buffer[position++]) & 0xff) << 0));
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
				if (c > 127) {
					break;
				}
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
					if (count > utflen) {
						throw new UTFDataFormatException("malformed input: partial character at end");
					}
					char2 = (int) bytearr[count - 1];
					if ((char2 & 0xC0) != 0x80) {
						throw new UTFDataFormatException("malformed input around byte " + count);
					}
					chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
					break;
				case 14:
					/* 1110 xxxx 10xx xxxx 10xx xxxx */
					count += 3;
					if (count > utflen) {
						throw new UTFDataFormatException("malformed input: partial character at end");
					}
					char2 = (int) bytearr[count - 2];
					char3 = (int) bytearr[count - 1];
					if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
						throw new UTFDataFormatException("malformed input around byte " + (count - 1));
					}
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
			if (this.position < this.end) {
				return (this.buffer[this.position++] & 0xff);
			} else {
				throw new EOFException();
			}
		}

		@Override
		public int readUnsignedShort() throws IOException {
			if (this.position < this.end - 1) {
				return ((this.buffer[this.position++] & 0xff) << 8) | ((this.buffer[this.position++] & 0xff) << 0);
			} else {
				throw new EOFException();
			}
		}
		
		@Override
		public int skipBytes(int n) throws IOException {
			if (this.position <= this.end - n) {
				this.position += n;
				return n;
			} else {
				n = this.end - this.position;
				this.position = this.end;
				return n;
			}
		}
		
		@SuppressWarnings("restriction")
		private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;
		
		@SuppressWarnings("restriction")
		private static final long BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
		
		private static final boolean LITTLE_ENDIAN = (MemoryUtils.NATIVE_BYTE_ORDER == ByteOrder.LITTLE_ENDIAN);

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			int skippedBytes = skipBytes(numBytes);

			if(skippedBytes < numBytes){
				throw new EOFException("Could not skip " + numBytes +" bytes.");
			}
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if(b == null){
				throw new NullPointerException("Byte array b cannot be null.");
			}

			if(off < 0){
				throw new IndexOutOfBoundsException("Offset cannot be negative.");
			}

			if(len < 0){
				throw new IndexOutOfBoundsException("Length cannot be negative.");
			}

			if(b.length - off < len){
				throw new IndexOutOfBoundsException("Byte array does not provide enough space to store requested data" +
						".");
			}

			if(this.position >= this.end){
				return -1;
			}else{
				int toRead = Math.min(this.end-this.position, len);
				System.arraycopy(this.buffer,this.position,b,off,toRead);
				this.position += toRead;

				return toRead;
			}
		}

		@Override
		public int read(byte[] b) throws IOException {
			return read(b, 0, b.length);
		}
	}
}
