/*
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
 */

package eu.stratosphere.api.common.io;

import eu.stratosphere.core.memory.DataInputView;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Wrapper to use ByteArrayInputStream with TypeSerializers
 */
public class ByteArrayInputView implements DataInputView{

	private final ByteArrayInputStream byteArrayInputStream;
	private final DataInputStream inputStream;

	public ByteArrayInputView(byte[] buffer){
		byteArrayInputStream = new ByteArrayInputStream(buffer);
		inputStream = new DataInputStream(byteArrayInputStream);
	}

	@Override
	public void skipBytesToRead(int numBytes) throws IOException {
		inputStream.skipBytes(numBytes);
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		inputStream.readFully(b);
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		inputStream.readFully(b, off, len);
	}

	@Override
	public int skipBytes(int n) throws IOException {
		return inputStream.skipBytes(n);
	}

	@Override
	public boolean readBoolean() throws IOException {
		return inputStream.readBoolean();
	}

	@Override
	public byte readByte() throws IOException {
		return inputStream.readByte();
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return inputStream.readUnsignedByte();
	}

	@Override
	public short readShort() throws IOException {
		return inputStream.readShort();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		return inputStream.readUnsignedShort();
	}

	@Override
	public char readChar() throws IOException {
		return inputStream.readChar();
	}

	@Override
	public int readInt() throws IOException {
		return inputStream.readInt();
	}

	@Override
	public long readLong() throws IOException {
		return inputStream.readLong();
	}

	@Override
	public float readFloat() throws IOException {
		return inputStream.readFloat();
	}

	@Override
	public double readDouble() throws IOException {
		return inputStream.readDouble();
	}

	@Override
	public String readLine() throws IOException {
		return inputStream.readLine();
	}

	@Override
	public String readUTF() throws IOException {
		return inputStream.readUTF();
	}
}
