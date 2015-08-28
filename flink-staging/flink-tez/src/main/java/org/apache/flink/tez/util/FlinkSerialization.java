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

package org.apache.flink.tez.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FlinkSerialization<T> extends Configured implements Serialization<T>{

	@Override
	public boolean accept(Class<?> c) {
		TypeSerializer<T> typeSerializer = (TypeSerializer) EncodingUtils.decodeObjectFromString(this.getConf().get("io.flink.typeserializer"), getClass().getClassLoader());
		T instance = typeSerializer.createInstance();
		return instance.getClass().isAssignableFrom(c);
	}

	@Override
	public Serializer<T> getSerializer(Class<T> c) {
		TypeSerializer<T> typeSerializer = (TypeSerializer) EncodingUtils.decodeObjectFromString(this.getConf().get("io.flink.typeserializer"), getClass().getClassLoader());
		return new FlinkSerializer<T>(typeSerializer);
	}

	@Override
	public Deserializer<T> getDeserializer(Class<T> c) {
		TypeSerializer<T> typeSerializer = (TypeSerializer) EncodingUtils.decodeObjectFromString(this.getConf().get("io.flink.typeserializer"), getClass().getClassLoader());
		return new FlinkDeserializer<T>(typeSerializer);
	}

	public static class FlinkSerializer<T> implements Serializer<T> {

		private OutputStream dataOut;
		private DataOutputViewOutputStreamWrapper dataOutputView;
		private TypeSerializer<T> typeSerializer;

		public FlinkSerializer(TypeSerializer<T> typeSerializer) {
			this.typeSerializer = typeSerializer;
		}

		@Override
		public void open(OutputStream out) throws IOException {
			this.dataOut = out;
			this.dataOutputView = new DataOutputViewOutputStreamWrapper(out);
		}

		@Override
		public void serialize(T t) throws IOException {
			typeSerializer.serialize(t, dataOutputView);
		}

		@Override
		public void close() throws IOException {
			this.dataOut.close();
		}
	}

	public static class FlinkDeserializer<T> implements Deserializer<T> {

		private InputStream dataIn;
		private TypeSerializer<T> typeSerializer;
		private DataInputViewInputStreamWrapper dataInputView;

		public FlinkDeserializer(TypeSerializer<T> typeSerializer) {
			this.typeSerializer = typeSerializer;
		}

		@Override
		public void open(InputStream in) throws IOException {
			this.dataIn = in;
			this.dataInputView = new DataInputViewInputStreamWrapper(in);
		}

		@Override
		public T deserialize(T t) throws IOException {
			T reuse = t;
			if (reuse == null) {
				reuse = typeSerializer.createInstance();
			}
			return typeSerializer.deserialize(reuse, dataInputView);
		}

		@Override
		public void close() throws IOException {
			this.dataIn.close();
		}
	}

	private static final class DataOutputViewOutputStreamWrapper implements DataOutputView {

		private final DataOutputStream dos;

		public DataOutputViewOutputStreamWrapper(OutputStream output) {
			this.dos = new DataOutputStream(output);
		}

		@Override
		public void write(int b) throws IOException {
			dos.write(b);
		}

		@Override
		public void write(byte[] b) throws IOException {
			dos.write(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			dos.write(b, off, len);
		}

		@Override
		public void writeBoolean(boolean v) throws IOException {
			dos.writeBoolean(v);
		}

		@Override
		public void writeByte(int v) throws IOException {
			dos.writeByte(v);
		}

		@Override
		public void writeShort(int v) throws IOException {
			dos.writeShort(v);
		}

		@Override
		public void writeChar(int v) throws IOException {
			dos.writeChar(v);
		}

		@Override
		public void writeInt(int v) throws IOException {
			dos.writeInt(v);
		}

		@Override
		public void writeLong(long v) throws IOException {
			dos.writeLong(v);
		}

		@Override
		public void writeFloat(float v) throws IOException {
			dos.writeFloat(v);
		}

		@Override
		public void writeDouble(double v) throws IOException {
			dos.writeDouble(v);
		}

		@Override
		public void writeBytes(String s) throws IOException {
			dos.writeBytes(s);
		}

		@Override
		public void writeChars(String s) throws IOException {
			dos.writeChars(s);
		}

		@Override
		public void writeUTF(String s) throws IOException {
			dos.writeUTF(s);
		}

		@Override
		public void skipBytesToWrite(int num) throws IOException {
			for (int i = 0; i < num; i++) {
				dos.write(0);
			}
		}

		@Override
		public void write(DataInputView inview, int num) throws IOException {
			for (int i = 0; i < num; i++) {
				dos.write(inview.readByte());
			}
		}
	}

	private static final class DataInputViewInputStreamWrapper implements DataInputView {

		private final DataInputStream dis;


		public DataInputViewInputStreamWrapper(InputStream input) {
			this.dis = new DataInputStream(input);
		}

		@Override
		public void readFully(byte[] b) throws IOException {
			dis.readFully(b);
		}

		@Override
		public void readFully(byte[] b, int off, int len) throws IOException {
			dis.readFully(b, off, len);
		}

		@Override
		public int skipBytes(int n) throws IOException {
			return dis.skipBytes(n);
		}

		@Override
		public boolean readBoolean() throws IOException {
			return dis.readBoolean();
		}

		@Override
		public byte readByte() throws IOException {
			return dis.readByte();
		}

		@Override
		public int readUnsignedByte() throws IOException {
			return dis.readUnsignedByte();
		}

		@Override
		public short readShort() throws IOException {
			return dis.readShort();
		}

		@Override
		public int readUnsignedShort() throws IOException {
			return dis.readUnsignedShort();
		}

		@Override
		public char readChar() throws IOException {
			return dis.readChar();
		}

		@Override
		public int readInt() throws IOException {
			return dis.readInt();
		}

		@Override
		public long readLong() throws IOException {
			return dis.readLong();
		}

		@Override
		public float readFloat() throws IOException {
			return dis.readFloat();
		}

		@Override
		public double readDouble() throws IOException {
			return dis.readDouble();
		}

		@Override
		public String readLine() throws IOException {
			return dis.readLine();
		}

		@Override
		public String readUTF() throws IOException {
			return dis.readUTF();
		}

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
			while (numBytes > 0) {
				numBytes -= dis.skipBytes(numBytes);
			}
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			dis.readFully(b, off, len);
			return len;
		}

		@Override
		public int read(byte[] b) throws IOException {
			return read(b, 0, b.length);
		}
	}

}
