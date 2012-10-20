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
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;


/**
 *
 *
 * @author Stephan Ewen
 */
public class SerializationDelegate<T> implements Record
{
	private T instance;
	
	private final TypeSerializer<T> serializer;
	
	private final OutputViewWrapper wrapper;
	
	
	public SerializationDelegate(TypeSerializer<T> serializer)
	{
		this.serializer = serializer;
		this.wrapper = new OutputViewWrapper();
	}
	
	public void setInstance(T instance) {
		this.instance = instance;
	}
	
	public T getInstance() {
		return this.instance;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.types.Record#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException
	{
		this.wrapper.setDelegate(out);
		this.serializer.serialize(this.instance, this.wrapper);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.types.Record#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException
	{
		throw new IllegalStateException("Deserialization method called on SerializationDelegate.");
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Utility class that wraps a {@link DataOutput} as a {@link DataOutputView}.
	 */
	private static final class OutputViewWrapper implements DataOutputView
	{
		private DataOutput delegate;
		
		public void setDelegate(DataOutput delegate) {
			this.delegate = delegate;
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#write(int)
		 */
		@Override
		public void write(int b) throws IOException {
			this.delegate.write(b);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#write(byte[])
		 */
		@Override
		public void write(byte[] b) throws IOException {
			this.delegate.write(b);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#write(byte[], int, int)
		 */
		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			this.delegate.write(b, off, len);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#writeBoolean(boolean)
		 */
		@Override
		public void writeBoolean(boolean v) throws IOException {
			this.delegate.writeBoolean(v);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#writeByte(int)
		 */
		@Override
		public void writeByte(int v) throws IOException {
			this.delegate.writeByte(v);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#writeShort(int)
		 */
		@Override
		public void writeShort(int v) throws IOException {
			this.delegate.writeShort(v);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#writeChar(int)
		 */
		@Override
		public void writeChar(int v) throws IOException {
			this.delegate.writeChar(v);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#writeInt(int)
		 */
		@Override
		public void writeInt(int v) throws IOException {
			this.delegate.writeInt(v);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#writeLong(long)
		 */
		@Override
		public void writeLong(long v) throws IOException {
			this.delegate.writeLong(v);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#writeFloat(float)
		 */
		@Override
		public void writeFloat(float v) throws IOException {
			this.delegate.writeFloat(v);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#writeDouble(double)
		 */
		@Override
		public void writeDouble(double v) throws IOException {
			this.delegate.writeDouble(v);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#writeBytes(java.lang.String)
		 */
		@Override
		public void writeBytes(String s) throws IOException {
			this.delegate.writeBytes(s);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#writeChars(java.lang.String)
		 */
		@Override
		public void writeChars(String s) throws IOException {
			this.delegate.writeChars(s);
		}

		/* (non-Javadoc)
		 * @see java.io.DataOutput#writeUTF(java.lang.String)
		 */
		@Override
		public void writeUTF(String s) throws IOException {
			this.delegate.writeUTF(s);
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.nephele.services.memorymanager.DataOutputView#skipBytesToWrite(int)
		 */
		@Override
		public void skipBytesToWrite(int numBytes) throws IOException {
			// skip by writing zeros.
			for (int i = 0; i < numBytes; i++) {
				this.delegate.writeByte(0);
			}
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.nephele.services.memorymanager.DataOutputView#write(eu.stratosphere.nephele.services.memorymanager.DataInputView, int)
		 */
		@Override
		public void write(DataInputView source, int numBytes) throws IOException {
			for (int i = 0; i < numBytes; i++) {
				this.delegate.writeByte(source.readByte());
			}
		}
	}
}
