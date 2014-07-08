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

package eu.stratosphere.core.memory;

import java.io.DataInput;
import java.io.IOException;

/**
 * A utility that presents a {@link DataInput} as a {@link DataInputView}.
 */
public class InputViewDataInputWrapper implements DataInputView {
	
	private DataInput delegate;
	
	public void setDelegate(DataInput delegate) {
		this.delegate = delegate;
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		this.delegate.readFully(b);
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		this.delegate.readFully(b, off, len);
	}

	@Override
	public int skipBytes(int n) throws IOException {
		return this.delegate.skipBytes(n);
	}

	@Override
	public boolean readBoolean() throws IOException {
		return this.delegate.readBoolean();
	}

	@Override
	public byte readByte() throws IOException {
		return this.delegate.readByte();
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return this.delegate.readUnsignedByte();
	}

	@Override
	public short readShort() throws IOException {
		return this.delegate.readShort();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		return this.delegate.readUnsignedShort();
	}

	@Override
	public char readChar() throws IOException {
		return this.delegate.readChar();
	}

	@Override
	public int readInt() throws IOException {
		return this.delegate.readInt();
	}

	@Override
	public long readLong() throws IOException {
		return this.delegate.readLong();
	}

	@Override
	public float readFloat() throws IOException {
		return this.delegate.readFloat();
	}

	@Override
	public double readDouble() throws IOException {
		return this.delegate.readDouble();
	}

	@Override
	public String readLine() throws IOException {
		return this.delegate.readLine();
	}

	@Override
	public String readUTF() throws IOException {
		return this.delegate.readUTF();
	}

	@Override
	public void skipBytesToRead(int numBytes) throws IOException {
		for (int i = 0; i < numBytes; i++) {
			this.delegate.readByte();
		}
	}
}
