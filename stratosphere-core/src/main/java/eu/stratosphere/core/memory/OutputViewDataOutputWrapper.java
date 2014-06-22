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

import java.io.DataOutput;
import java.io.IOException;

/**
 * A utility that presents a {@link DataOutput} as a {@link DataOutputView}.
 */
public class OutputViewDataOutputWrapper implements DataOutputView {
	
	private DataOutput delegate;
	
	public void setDelegate(DataOutput delegate) {
		this.delegate = delegate;
	}

	@Override
	public void write(int b) throws IOException {
		this.delegate.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		this.delegate.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		this.delegate.write(b, off, len);
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		this.delegate.writeBoolean(v);
	}

	@Override
	public void writeByte(int v) throws IOException {
		this.delegate.writeByte(v);
	}

	@Override
	public void writeShort(int v) throws IOException {
		this.delegate.writeShort(v);
	}

	@Override
	public void writeChar(int v) throws IOException {
		this.delegate.writeChar(v);
	}

	@Override
	public void writeInt(int v) throws IOException {
		this.delegate.writeInt(v);
	}

	@Override
	public void writeLong(long v) throws IOException {
		this.delegate.writeLong(v);
	}

	@Override
	public void writeFloat(float v) throws IOException {
		this.delegate.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) throws IOException {
		this.delegate.writeDouble(v);
	}

	@Override
	public void writeBytes(String s) throws IOException {
		this.delegate.writeBytes(s);
	}

	@Override
	public void writeChars(String s) throws IOException {
		this.delegate.writeChars(s);
	}

	@Override
	public void writeUTF(String s) throws IOException {
		this.delegate.writeUTF(s);
	}

	@Override
	public void skipBytesToWrite(int numBytes) throws IOException {
		// skip by writing zeros.
		for (int i = 0; i < numBytes; i++) {
			this.delegate.writeByte(0);
		}
	}

	@Override
	public void write(DataInputView source, int numBytes) throws IOException {
		for (int i = 0; i < numBytes; i++) {
			this.delegate.writeByte(source.readUnsignedByte());
		}
	}
}
