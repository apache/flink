/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.api.avro;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;


public class DataInputDecoder extends Decoder {
	
	private final Utf8 stringDecoder = new Utf8();
	
	private DataInput in;
	
	public void setIn(DataInput in) {
		this.in = in;
	}

	
	@Override
	public void readNull() throws IOException {}

	@Override
	public boolean readBoolean() throws IOException {
		return in.readBoolean();
	}

	@Override
	public int readInt() throws IOException {
		return in.readInt();
	}

	@Override
	public long readLong() throws IOException {
		return in.readLong();
	}

	@Override
	public float readFloat() throws IOException {
		return in.readFloat();
	}

	@Override
	public double readDouble() throws IOException {
		return in.readDouble();
	}

	@Override
	public Utf8 readString(Utf8 old) throws IOException {
		int length = readInt();
		Utf8 result = (old != null ? old : new Utf8());
		result.setByteLength(length);
		if (0 != length) {
			readBytedChecked(result.getBytes(), 0, length);
		}
		return result;
	}

	@Override
	public String readString() throws IOException {
		return readString(stringDecoder).toString();
	}

	@Override
	public void skipString() throws IOException {
		int len = readInt();
		skipBytes(len);
	}

	@Override
	public ByteBuffer readBytes(ByteBuffer old) throws IOException {
		int length = readInt();
		ByteBuffer result;
		if (old != null && length <= old.capacity() && old.hasArray()) {
			result = old;
			result.clear();
		} else {
			result = ByteBuffer.allocate(length);
		}
		readBytedChecked(result.array(), result.position(), length);
		result.limit(length);
		return result;
	}

	@Override
	public void skipBytes() throws IOException {
		int num = readInt();
		skipBytes(num);
	}

	@Override
	public void readFixed(byte[] bytes, int start, int length) throws IOException {
		readBytedChecked(bytes, start, length);
	}

	@Override
	public void skipFixed(int length) throws IOException {
		skipBytes(length);
	}

	@Override
	public int readEnum() throws IOException {
		return readInt();
	}

	@Override
	public long readArrayStart() throws IOException {
		return readLong();
	}

	@Override
	public long arrayNext() throws IOException {
		return readLong();
	}

	@Override
	public long skipArray() throws IOException {
		return skipItems();
	}

	@Override
	public long readMapStart() throws IOException {
		return readLong();
	}

	@Override
	public long mapNext() throws IOException {
		return readLong();
	}

	@Override
	public long skipMap() throws IOException {
		return skipItems();
	}

	@Override
	public int readIndex() throws IOException {
		return readInt();
	}
	
	private void readBytedChecked(byte[] bytes, int start, int length) throws IOException {
		if (length < 0)
			throw new IOException("Malformed input.");
		
		in.readFully(bytes, start, length);
	}
	
	private void skipBytes(int num) throws IOException {
		while (num > 0) {
			num -= in.skipBytes(num);
		}
	}
	
	private void skipBytes(long num) throws IOException {
		while (num > 0) {
			num -= in.skipBytes((int) (num < Integer.MAX_VALUE ? num : Integer.MAX_VALUE));
		}
	}
	
	private long skipItems() throws IOException {
		long result = readInt();
		while (result < 0) {
			long bytecount = readLong();
			skipBytes(bytecount);
			result = readInt();
		}
		return result;
	}
}
