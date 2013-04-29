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

package eu.stratosphere.pact.runtime.plugable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.generic.types.TypeSerializer;


/**
 *
 *
 * @author Stephan Ewen
 */
public class DeserializationDelegate<T> implements Record
{
	private T instance;
	
	private final TypeSerializer<T> serializer;
	
	private final InputViewWrapper wrapper;
	
	
	public DeserializationDelegate(TypeSerializer<T> serializer)
	{
		this.serializer = serializer;
		this.wrapper = new InputViewWrapper();
	}
	
	public void setInstance(T instance) {
		this.instance = instance;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		throw new IllegalStateException("Serialization method called on DeserializationDelegate.");
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException
	{
		this.wrapper.setDelegate(in);
		this.serializer.deserialize(this.instance, this.wrapper);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Utility class that wraps a {@link DataInput} as a {@link DataInputView}.
	 */
	private static final class InputViewWrapper implements DataInputView
	{
		private DataInput delegate;
		
		public void setDelegate(DataInput delegate) {
			this.delegate = delegate;
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readFully(byte[])
		 */
		@Override
		public void readFully(byte[] b) throws IOException {
			this.delegate.readFully(b);
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readFully(byte[], int, int)
		 */
		@Override
		public void readFully(byte[] b, int off, int len) throws IOException {
			this.delegate.readFully(b, off, len);
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#skipBytes(int)
		 */
		@Override
		public int skipBytes(int n) throws IOException {
			return this.delegate.skipBytes(n);
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readBoolean()
		 */
		@Override
		public boolean readBoolean() throws IOException {
			return this.delegate.readBoolean();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readByte()
		 */
		@Override
		public byte readByte() throws IOException {
			return this.delegate.readByte();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readUnsignedByte()
		 */
		@Override
		public int readUnsignedByte() throws IOException {
			return this.delegate.readUnsignedByte();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readShort()
		 */
		@Override
		public short readShort() throws IOException {
			return this.delegate.readShort();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readUnsignedShort()
		 */
		@Override
		public int readUnsignedShort() throws IOException {
			return this.delegate.readUnsignedShort();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readChar()
		 */
		@Override
		public char readChar() throws IOException {
			return this.delegate.readChar();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readInt()
		 */
		@Override
		public int readInt() throws IOException {
			return this.delegate.readInt();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readLong()
		 */
		@Override
		public long readLong() throws IOException {
			return this.delegate.readLong();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readFloat()
		 */
		@Override
		public float readFloat() throws IOException {
			return this.delegate.readFloat();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readDouble()
		 */
		@Override
		public double readDouble() throws IOException {
			return this.delegate.readDouble();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readLine()
		 */
		@Override
		public String readLine() throws IOException {
			return this.delegate.readLine();
		}

		/* (non-Javadoc)
		 * @see java.io.DataInput#readUTF()
		 */
		@Override
		public String readUTF() throws IOException {
			return this.delegate.readUTF();
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.nephele.services.memorymanager.DataInputView#skipBytesToRead(int)
		 */
		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			for (int i = 0; i < numBytes; i++) {
				this.delegate.readByte();
			}
		}
	}
}
