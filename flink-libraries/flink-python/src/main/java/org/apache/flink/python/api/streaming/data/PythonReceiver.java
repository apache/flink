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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.api.PythonOptions;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * This class is used to read data from memory-mapped files.
 */
public class PythonReceiver<OUT> implements Serializable {
	private static final long serialVersionUID = -2474088929850009968L;

	private transient RandomAccessFile inputRAF;
	private transient FileChannel inputChannel;
	private transient MappedByteBuffer fileBuffer;

	private final long mappedFileSizeBytes;

	private final boolean readAsByteArray;

	private transient Deserializer<OUT> deserializer;

	public PythonReceiver(Configuration config, boolean usesByteArray) {
		readAsByteArray = usesByteArray;
		mappedFileSizeBytes = config.getLong(PythonOptions.MMAP_FILE_SIZE) << 10;
	}

	//=====Setup========================================================================================================

	@SuppressWarnings("unchecked")
	public void open(File inputFile) throws IOException {
		deserializer = (Deserializer<OUT>) (readAsByteArray ? new ByteArrayDeserializer() : new TupleDeserializer());

		inputFile.getParentFile().mkdirs();

		if (inputFile.exists()) {
			inputFile.delete();
		}
		inputFile.createNewFile();
		inputRAF = new RandomAccessFile(inputFile, "rw");
		inputRAF.setLength(mappedFileSizeBytes);
		inputRAF.seek(mappedFileSizeBytes - 1);
		inputRAF.writeByte(0);
		inputRAF.seek(0);
		inputChannel = inputRAF.getChannel();
		fileBuffer = inputChannel.map(FileChannel.MapMode.READ_WRITE, 0, mappedFileSizeBytes);
	}

	public void close() throws IOException {
		closeMappedFile();
	}

	private void closeMappedFile() throws IOException {
		inputChannel.close();
		inputRAF.close();
	}


	//=====IO===========================================================================================================
	/**
	 * Reads a buffer of the given size from the memory-mapped file, and collects all records contained. This method
	 * assumes that all values in the buffer are of the same type. This method does NOT take care of synchronization.
	 * The user must guarantee that the buffer was completely written before calling this method.
	 *
	 * @param c Collector to collect records
	 * @param bufferSize size of the buffer
	 * @throws IOException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void collectBuffer(Collector<OUT> c, int bufferSize) throws IOException {
		fileBuffer.position(0);

		while (fileBuffer.position() < bufferSize) {
			c.collect(deserializer.deserialize());
		}
	}

	//=====Deserializer=================================================================================================
	private interface Deserializer<T> {
		T deserialize();
	}

	private class ByteArrayDeserializer implements Deserializer<byte[]> {
		@Override
		public byte[] deserialize() {
			int size = fileBuffer.getInt();
			byte[] value = new byte[size];
			fileBuffer.get(value);
			return value;
		}
	}

	private class TupleDeserializer implements Deserializer<Tuple2<Tuple, byte[]>> {
		@Override
		public Tuple2<Tuple, byte[]> deserialize() {
			int keyTupleSize = fileBuffer.get();
			Tuple keys = createTuple(keyTupleSize);
			for (int x = 0; x < keyTupleSize; x++) {
				byte[] data = new byte[fileBuffer.getInt()];
				fileBuffer.get(data);
				keys.setField(data, x);
			}
			byte[] value = new byte[fileBuffer.getInt()];
			fileBuffer.get(value);
			return new Tuple2<>(keys, value);
		}
	}

	public static Tuple createTuple(int size) {
		try {
			return Tuple.getTupleClass(size).newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
