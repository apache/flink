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
import eu.stratosphere.core.memory.DataOutputView;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Wrapper class to use ByteArrayOutputStream with TypeSerializers.
 */
public class ByteArrayOutputView implements DataOutputView {
	private final ByteArrayOutputStream byteOutputStream;
	private final DataOutputStream outputStream;

	public ByteArrayOutputView(){
		byteOutputStream = new ByteArrayOutputStream();
		outputStream = new DataOutputStream(byteOutputStream);
	}

	public byte[] getByteArray(){
		return byteOutputStream.toByteArray();
	}

	public void reset() {
		byteOutputStream.reset();
	}

	@Override
	public void skipBytesToWrite(int numBytes) throws IOException {
		for(int i=0; i<numBytes; i++){
			writeByte(0);
		}
	}

	@Override
	public void write(DataInputView source, int numBytes) throws IOException {
		byte[] buffer = new byte[numBytes];
		source.readFully(buffer);
		outputStream.write(buffer);
	}

	@Override
	public void write(int b) throws IOException {
		outputStream.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		outputStream.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		outputStream.write(b, off, len);
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		outputStream.writeBoolean(v);
	}

	@Override
	public void writeByte(int v) throws IOException {
		outputStream.writeByte(v);
	}

	@Override
	public void writeShort(int v) throws IOException {
		outputStream.writeShort(v);
	}

	@Override
	public void writeChar(int v) throws IOException {
		outputStream.writeChar(v);
	}

	@Override
	public void writeInt(int v) throws IOException {
		outputStream.writeInt(v);
	}

	@Override
	public void writeLong(long v) throws IOException {
		outputStream.writeLong(v);
	}

	@Override
	public void writeFloat(float v) throws IOException {
		outputStream.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) throws IOException {
		outputStream.writeDouble(v);
	}

	@Override
	public void writeBytes(String s) throws IOException {
		outputStream.writeBytes(s);
	}

	@Override
	public void writeChars(String s) throws IOException {
		outputStream.writeChars(s);
	}

	@Override
	public void writeUTF(String s) throws IOException {
		outputStream.writeUTF(s);
	}
}
