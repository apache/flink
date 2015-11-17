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
package org.apache.flink.python.api.streaming;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.tuple.Tuple;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_TMP_DATA_DIR;
import static org.apache.flink.python.api.PythonPlanBinder.MAPPED_FILE_SIZE;
import static org.apache.flink.python.api.streaming.Sender.TYPE_BOOLEAN;
import static org.apache.flink.python.api.streaming.Sender.TYPE_BYTE;
import static org.apache.flink.python.api.streaming.Sender.TYPE_BYTES;
import static org.apache.flink.python.api.streaming.Sender.TYPE_DOUBLE;
import static org.apache.flink.python.api.streaming.Sender.TYPE_FLOAT;
import static org.apache.flink.python.api.streaming.Sender.TYPE_INTEGER;
import static org.apache.flink.python.api.streaming.Sender.TYPE_LONG;
import static org.apache.flink.python.api.streaming.Sender.TYPE_NULL;
import static org.apache.flink.python.api.streaming.Sender.TYPE_SHORT;
import static org.apache.flink.python.api.streaming.Sender.TYPE_STRING;
import static org.apache.flink.python.api.streaming.Sender.TYPE_TUPLE;
import org.apache.flink.python.api.types.CustomTypeWrapper;
import org.apache.flink.util.Collector;

/**
 * General-purpose class to read data from memory-mapped files.
 */
public class Receiver implements Serializable {
	private static final long serialVersionUID = -2474088929850009968L;

	private final AbstractRichFunction function;

	private File inputFile;
	private RandomAccessFile inputRAF;
	private FileChannel inputChannel;
	private MappedByteBuffer fileBuffer;

	private Deserializer<?> deserializer = null;

	public Receiver(AbstractRichFunction function) {
		this.function = function;
	}

	//=====Setup========================================================================================================
	public void open(String path) throws IOException {
		setupMappedFile(path);
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

	//=====Record-API===================================================================================================
	/**
	 * Loads a buffer from the memory-mapped file. The records contained within the buffer can be accessed using
	 * collectRecord(). These records do not necessarily have to be of the same type. This method requires external
	 * synchronization.
	 *
	 * @throws IOException
	 */
	private void loadBuffer() throws IOException {
		int count = 0;
		while (fileBuffer.get(0) == 0 && count < 10) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ie) {
			}
			fileBuffer.load();
			count++;
		}
		if (fileBuffer.get(0) == 0) {
			throw new RuntimeException("External process not responding.");
		}
		fileBuffer.position(1);
	}

	/**
	 * Returns a record from the buffer. Note: This method cannot be replaced with specific methods like readInt() or
	 * similar. The PlanBinder requires a method that can return any kind of object.
	 *
	 * @return read record
	 * @throws IOException
	 */
	public Object getRecord() throws IOException {
		return getRecord(false);
	}

	/**
	 * Returns a record from the buffer. Note: This method cannot be replaced with specific methods like readInt() or
	 * similar. The PlanBinder requires a method that can return any kind of object.
	 *
	 * @param normalized flag indicating whether certain types should be normalized
	 * @return read record
	 * @throws IOException
	 */
	public Object getRecord(boolean normalized) throws IOException {
		if (fileBuffer.position() == 0) {
			loadBuffer();
		}
		return receiveField(normalized);
	}

	/**
	 * Reads a single primitive value or tuple from the buffer.
	 *
	 * @return primitive value or tuple
	 * @throws IOException
	 */
	private Object receiveField(boolean normalized) throws IOException {
		byte type = fileBuffer.get();
		switch (type) {
			case TYPE_TUPLE:
				int tupleSize = fileBuffer.get();
				Tuple tuple = createTuple(tupleSize);
				for (int x = 0; x < tupleSize; x++) {
					tuple.setField(receiveField(normalized), x);
				}
				return tuple;
			case TYPE_BOOLEAN:
				return fileBuffer.get() == 1;
			case TYPE_BYTE:
				return fileBuffer.get();
			case TYPE_SHORT:
				if (normalized) {
					return (int) fileBuffer.getShort();
				} else {
					return fileBuffer.getShort();
				}
			case TYPE_INTEGER:
				return fileBuffer.getInt();
			case TYPE_LONG:
				if (normalized) {
					return new Long(fileBuffer.getLong()).intValue();
				} else {
					return fileBuffer.getLong();
				}
			case TYPE_FLOAT:
				if (normalized) {
					return (double) fileBuffer.getFloat();
				} else {
					return fileBuffer.getFloat();
				}
			case TYPE_DOUBLE:
				return fileBuffer.getDouble();
			case TYPE_STRING:
				int stringSize = fileBuffer.getInt();
				byte[] buffer = new byte[stringSize];
				fileBuffer.get(buffer);
				return new String(buffer);
			case TYPE_BYTES:
				int bytessize = fileBuffer.getInt();
				byte[] bytebuffer = new byte[bytessize];
				fileBuffer.get(bytebuffer);
				return bytebuffer;
			case TYPE_NULL:
				return null;
			default:
				return new CustomTypeDeserializer(type).deserialize();
		}
	}

	//=====Buffered-API=================================================================================================
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

		if (deserializer == null) {
			byte type = fileBuffer.get();
			deserializer = getDeserializer(type);
		}
		while (fileBuffer.position() < bufferSize) {
			c.collect(deserializer.deserialize());
		}
	}

	//=====Deserializer=================================================================================================
	private Deserializer<?> getDeserializer(byte type) {
		switch (type) {
			case TYPE_TUPLE:
				return new TupleDeserializer();
			case TYPE_BOOLEAN:
				return new BooleanDeserializer();
			case TYPE_BYTE:
				return new ByteDeserializer();
			case TYPE_BYTES:
				return new BytesDeserializer();
			case TYPE_SHORT:
				return new ShortDeserializer();
			case TYPE_INTEGER:
				return new IntDeserializer();
			case TYPE_LONG:
				return new LongDeserializer();
			case TYPE_STRING:
				return new StringDeserializer();
			case TYPE_FLOAT:
				return new FloatDeserializer();
			case TYPE_DOUBLE:
				return new DoubleDeserializer();
			case TYPE_NULL:
				return new NullDeserializer();
			default:
				return new CustomTypeDeserializer(type);

		}
	}

	private interface Deserializer<T> {
		public T deserialize();
	}

	private class CustomTypeDeserializer implements Deserializer<CustomTypeWrapper> {
		private final byte type;

		public CustomTypeDeserializer(byte type) {
			this.type = type;
		}

		@Override
		public CustomTypeWrapper deserialize() {
			int size = fileBuffer.getInt();
			byte[] data = new byte[size];
			fileBuffer.get(data);
			return new CustomTypeWrapper(type, data);
		}
	}

	private class BooleanDeserializer implements Deserializer<Boolean> {
		@Override
		public Boolean deserialize() {
			return fileBuffer.get() == 1;
		}
	}

	private class ByteDeserializer implements Deserializer<Byte> {
		@Override
		public Byte deserialize() {
			return fileBuffer.get();
		}
	}

	private class ShortDeserializer implements Deserializer<Short> {
		@Override
		public Short deserialize() {
			return fileBuffer.getShort();
		}
	}

	private class IntDeserializer implements Deserializer<Integer> {
		@Override
		public Integer deserialize() {
			return fileBuffer.getInt();
		}
	}

	private class LongDeserializer implements Deserializer<Long> {
		@Override
		public Long deserialize() {
			return fileBuffer.getLong();
		}
	}

	private class FloatDeserializer implements Deserializer<Float> {
		@Override
		public Float deserialize() {
			return fileBuffer.getFloat();
		}
	}

	private class DoubleDeserializer implements Deserializer<Double> {
		@Override
		public Double deserialize() {
			return fileBuffer.getDouble();
		}
	}

	private class StringDeserializer implements Deserializer<String> {
		private int size;

		@Override
		public String deserialize() {
			size = fileBuffer.getInt();
			byte[] buffer = new byte[size];
			fileBuffer.get(buffer);
			return new String(buffer);
		}
	}

	private class NullDeserializer implements Deserializer<Object> {
		@Override
		public Object deserialize() {
			return null;
		}
	}

	private class BytesDeserializer implements Deserializer<byte[]> {
		@Override
		public byte[] deserialize() {
			int length = fileBuffer.getInt();
			byte[] result = new byte[length];
			fileBuffer.get(result);
			return result;
		}

	}

	private class TupleDeserializer implements Deserializer<Tuple> {
		Deserializer<?>[] deserializer = null;
		Tuple reuse;

		public TupleDeserializer() {
			int size = fileBuffer.getInt();
			reuse = createTuple(size);
			deserializer = new Deserializer[size];
			for (int x = 0; x < deserializer.length; x++) {
				deserializer[x] = getDeserializer(fileBuffer.get());
			}
		}

		@Override
		public Tuple deserialize() {
			for (int x = 0; x < deserializer.length; x++) {
				reuse.setField(deserializer[x].deserialize(), x);
			}
			return reuse;
		}
	}

	public static Tuple createTuple(int size) {
		try {
			return Tuple.getTupleClass(size).newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
