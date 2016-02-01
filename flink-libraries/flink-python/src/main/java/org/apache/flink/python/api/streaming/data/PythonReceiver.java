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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_TMP_DATA_DIR;
import static org.apache.flink.python.api.PythonPlanBinder.MAPPED_FILE_SIZE;
import org.apache.flink.util.Collector;

/**
 * This class is used to read data from memory-mapped files.
 */
public class PythonReceiver implements Serializable {
	private static final long serialVersionUID = -2474088929850009968L;

	private File inputFile;
	private RandomAccessFile inputRAF;
	private FileChannel inputChannel;
	private MappedByteBuffer fileBuffer;

	private final boolean readAsByteArray;

	private Deserializer<?> deserializer = null;

	public PythonReceiver(boolean usesByteArray) {
		readAsByteArray = usesByteArray;
	}

	//=====Setup========================================================================================================
	public void open(String path) throws IOException {
		setupMappedFile(path);
		deserializer = readAsByteArray ? new ByteArrayDeserializer() : new TupleDeserializer();
	}

	private void setupMappedFile(String inputFilePath) throws FileNotFoundException, IOException {
		File x = new File(FLINK_TMP_DATA_DIR);
		x.mkdirs();

		inputFile = new File(inputFilePath);
		if (inputFile.exists()) {
			inputFile.delete();
		}
		inputFile.createNewFile();
		inputRAF = new RandomAccessFile(inputFilePath, "rw");
		inputRAF.setLength(MAPPED_FILE_SIZE);
		inputRAF.seek(MAPPED_FILE_SIZE - 1);
		inputRAF.writeByte(0);
		inputRAF.seek(0);
		inputChannel = inputRAF.getChannel();
		fileBuffer = inputChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAPPED_FILE_SIZE);
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
	public void collectBuffer(Collector c, int bufferSize) throws IOException {
		fileBuffer.position(0);

		while (fileBuffer.position() < bufferSize) {
			c.collect(deserializer.deserialize());
		}
	}

	//=====Deserializer=================================================================================================
	private interface Deserializer<T> {
		public T deserialize();
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
