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

package org.apache.flink.test.manual;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class PojoSerializationPerformanceTest {
	private static final class TestPojo {
		public int key;
		public long value;
	}

	@SuppressWarnings("NullableProblems")
	private static class TestDataOutputView implements DataOutputView {
		public TestDataOutputView() {
		}

		@Override
		public void skipBytesToWrite(int numBytes) throws IOException {
		}

		@Override
		public void write(DataInputView source, int numBytes) throws IOException {
		}

		@Override
		public void write(int b) throws IOException {
		}

		@Override
		public void write(byte[] b) throws IOException {
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
		}

		@Override
		public void writeBoolean(boolean v) throws IOException {
		}

		@Override
		public void writeByte(int v) throws IOException {
		}

		@Override
		public void writeShort(int v) throws IOException {
		}

		@Override
		public void writeChar(int v) throws IOException {
		}

		@Override
		public void writeInt(int v) throws IOException {
		}

		@Override
		public void writeLong(long v) throws IOException {
		}

		@Override
		public void writeFloat(float v) throws IOException {
		}

		@Override
		public void writeDouble(double v) throws IOException {
		}

		@Override
		public void writeBytes(String s) throws IOException {
		}

		@Override
		public void writeChars(String s) throws IOException {
		}

		@Override
		public void writeUTF(String s) throws IOException {
		}
	}

	@SuppressWarnings("NullableProblems")
	private static class TestDataInputView implements DataInputView {
		public TestDataInputView() {
		}

		@Override
		public boolean readBoolean() throws IOException {
			return true;
		}

		@Override
		public byte readByte() throws IOException {
			return 0;
		}

		@Override
		public char readChar() throws IOException {
			return 'c';
		}

		@Override
		public double readDouble() throws IOException {
			return 0;
		}

		@Override
		public float readFloat() throws IOException {
			return 0;
		}

		@Override
		public void readFully(byte[] b) throws IOException {
			readFully(b, 0, b.length);
		}

		@Override
		public void readFully(byte[] b, int off, int len) throws IOException {
			for(int j = off; j < len; ++j) {
				b[j] = 0;
			}
		}

		@Override
		public int readInt() throws IOException {
			return 0;
		}

		@Override
		public String readLine() throws IOException {
			return "";
		}

		@Override
		public long readLong() throws IOException {
			return 0;
		}

		@Override
		public short readShort() throws IOException {
			return 0;
		}

		@Override
		public String readUTF() throws IOException {
			return "";
		}

		@Override
		public int readUnsignedByte() throws IOException {
			return 0;
		}

		@Override
		public int readUnsignedShort() throws IOException {
			return 0;
		}

		@Override
		public int skipBytes(int n) throws IOException {
			return n;
		}

		@Override
		public void skipBytesToRead(int numBytes) throws IOException {
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			for(int j = off; j < len; ++j) {
				b[j] = 0;
			}
			return len - off;
		}

		@Override
		public int read(byte[] b) throws IOException {
			return read(b, 0, b.length);
		}
	}

	public static void main(String[] args) throws Exception {
		TypeInformation<TestPojo> testPojoTypeInfo = TypeInformation.of(TestPojo.class);
		ExecutionConfig config = new ExecutionConfig();
		config.disableCodeGeneration();
		TypeSerializer<TestPojo> originalSerializer = testPojoTypeInfo.createSerializer(config);
		config.enableCodeGeneration();
		TypeSerializer<TestPojo> generatedSerializer = testPojoTypeInfo.createSerializer(config);

		TestDataInputView mockedInput = new TestDataInputView();
		TestDataOutputView mockedOutput = new TestDataOutputView();
		TestPojo obj = new TestPojo();
		for (int j = 0; j < 10000; ++j) {
			obj.key = 0;
			obj.value = obj.key*2;
			testPerformance(obj, mockedOutput, mockedInput, generatedSerializer, 100);
		}
		long start = System.currentTimeMillis();
		for (int j = 0; j < 100; ++j) {
			obj.key = 0;
			obj.value = obj.key*2;
			testPerformance(obj, mockedOutput, mockedInput, generatedSerializer, 1000000);
		}
		long end = System.currentTimeMillis();
		System.out.println("### Total time of generated serialization: " + (end - start) + "");
		for (int j = 0; j < 10000; ++j) {
			obj.key = 0;
			obj.value = obj.key*2;
			testPerformance(obj, mockedOutput, mockedInput, originalSerializer, 100);
		}
		start = System.currentTimeMillis();
		for (int j = 0; j < 100; ++j) {
			obj.key = 0;
			obj.value = obj.key*2;
			testPerformance(obj, mockedOutput, mockedInput, originalSerializer, 1000000);
		}
		end = System.currentTimeMillis();
		System.out.println("### Total time of Flink serialization: " + (end - start) + "");
	}

	private static void testPerformance(TestPojo obj, DataOutputView outputView, DataInputView
		inputView, TypeSerializer<TestPojo> serializer, int num) throws IOException {
		for (int i = 0; i < num; ++i) {
			serializer.serialize(obj, outputView);
			obj = serializer.deserialize(inputView);
			obj.key++;
			obj.value = obj.key*2;
		}
	}
}
