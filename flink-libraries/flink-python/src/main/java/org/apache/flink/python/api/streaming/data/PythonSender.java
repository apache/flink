/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.python.api.streaming.data;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static org.apache.flink.python.api.PythonPlanBinder.FLINK_TMP_DATA_DIR;
import static org.apache.flink.python.api.PythonPlanBinder.MAPPED_FILE_SIZE;

/**
 * General-purpose class to write data to memory-mapped files.
 */
public abstract class PythonSender implements Serializable {

	private static final long serialVersionUID = -2004095650353962110L;

	public static final byte TYPE_ARRAY = 63;
	public static final byte TYPE_KEY_VALUE = 62;
	public static final byte TYPE_VALUE_VALUE = 61;

	private transient File outputFile;
	private transient RandomAccessFile outputRAF;
	private transient FileChannel outputChannel;
	private transient MappedByteBuffer fileBuffer;

	//=====Setup========================================================================================================
	public void open(String path) throws IOException {
		setupMappedFile(path);
	}

	private void setupMappedFile(String outputFilePath) throws IOException {
		File x = new File(FLINK_TMP_DATA_DIR);
		x.mkdirs();

		outputFile = new File(outputFilePath);
		if (outputFile.exists()) {
			outputFile.delete();
		}
		outputFile.createNewFile();
		outputRAF = new RandomAccessFile(outputFilePath, "rw");
		outputRAF.setLength(MAPPED_FILE_SIZE);
		outputRAF.seek(MAPPED_FILE_SIZE - 1);
		outputRAF.writeByte(0);
		outputRAF.seek(0);
		outputChannel = outputRAF.getChannel();
		fileBuffer = outputChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAPPED_FILE_SIZE);
	}

	public void close() throws IOException {
		closeMappedFile();
	}

	private void closeMappedFile() throws IOException {
		outputChannel.close();
		outputRAF.close();
		outputFile.delete();
	}

	//=====IO===========================================================================================================
	/**
	 * Extracts records from an iterator and writes them to the memory-mapped file. This method assumes that all values
	 * in the iterator are of the same type. This method does NOT take care of synchronization. The caller must
	 * guarantee that the file may be written to before calling this method.
	 *
	 * @param input     iterator containing records
	 * @param serializer serializer for the input records
	 * @return size of the written buffer
	 * @throws IOException
	 */
	protected <IN> int sendBuffer(SingleElementPushBackIterator<IN> input, Serializer<IN> serializer) throws IOException {
		fileBuffer.clear();

		while (input.hasNext()) {
			IN value = input.next();
			ByteBuffer bb = serializer.serialize(value);
			if (bb.remaining() > MAPPED_FILE_SIZE) {
				throw new RuntimeException("Serialized object does not fit into a single buffer.");
			}
			if (bb.remaining() <= fileBuffer.remaining()) {
				fileBuffer.put(bb);
			} else {
				input.pushBack(value);
				break;
			}
		}

		int size = fileBuffer.position();
		return size;
	}

	//=====Serializer===================================================================================================

	@SuppressWarnings("unchecked")
	protected <IN> Serializer<IN> getSerializer(IN value) {
		if (value instanceof byte[]) {
			return (Serializer<IN>) new ArraySerializer();
		}
		if (((Tuple2<?, ?>) value).f0 instanceof byte[]) {
			return (Serializer<IN>) new ValuePairSerializer();
		}
		if (((Tuple2<?, ?>) value).f0 instanceof Tuple) {
			return (Serializer<IN>) new KeyValuePairSerializer();
		}
		throw new IllegalArgumentException("This object can't be serialized: " + value);
	}

	protected abstract static class Serializer<T> {
		protected ByteBuffer buffer;

		public ByteBuffer serialize(T value) {
			serializeInternal(value);
			buffer.flip();
			return buffer;
		}

		public abstract void serializeInternal(T value);
	}

	private static class ArraySerializer extends Serializer<byte[]> {
		@Override
		public void serializeInternal(byte[] value) {
			buffer = ByteBuffer.allocate(value.length + 1);
			buffer.put(TYPE_ARRAY);
			buffer.put(value);
		}
	}

	private static class ValuePairSerializer extends Serializer<Tuple2<byte[], byte[]>> {
		@Override
		public void serializeInternal(Tuple2<byte[], byte[]> value) {
			buffer = ByteBuffer.allocate(1 + value.f0.length + value.f1.length);
			buffer.put(TYPE_VALUE_VALUE);
			buffer.put(value.f0);
			buffer.put(value.f1);
		}
	}

	private static class KeyValuePairSerializer extends Serializer<Tuple2<Tuple, byte[]>> {
		@Override
		public void serializeInternal(Tuple2<Tuple, byte[]> value) {
			int keySize = 0;
			for (int x = 0; x < value.f0.getArity(); x++) {
				keySize += ((byte[]) value.f0.getField(x)).length;
			}
			buffer = ByteBuffer.allocate(5 + keySize + value.f1.length);
			buffer.put(TYPE_KEY_VALUE);
			buffer.put((byte) value.f0.getArity());
			for (int x = 0; x < value.f0.getArity(); x++) {
				buffer.put((byte[]) value.f0.getField(x));
			}
			buffer.put(value.f1);
		}
	}
}
