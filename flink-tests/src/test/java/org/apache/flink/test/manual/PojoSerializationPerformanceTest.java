package org.apache.flink.test.manual;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.memory.ListMemorySegmentSource;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;


public class PojoSerializationPerformanceTest {
	public static final class TestPojo {
		public int key;
		public long value;
	}

	private static final int SEGMENT_SIZE = 1024*1024;


	public static class TestDataOutputView implements DataOutputView {
		private byte[] buffer;

		public TestDataOutputView(int size) {
			buffer = new byte[1];
		}

		public void clear() {
		}

		public byte[] getBuffer() {
			return buffer;
		}

		public void checkSize(int numBytes) throws EOFException {
		}

		@Override
		public void skipBytesToWrite(int numBytes) throws IOException {
			checkSize(numBytes);
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

	public static class TestDataInputView implements DataInputView {

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

	private static ArrayList<MemorySegment> getMemory(int numSegments, int segmentSize) {
		ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(numSegments);
		for (int i = 0; i < numSegments; i++) {
			list.add(MemorySegmentFactory.allocateUnpooledSegment(segmentSize));
		}
		return list;
	}

	public static void main(String[] args) throws Exception {
		TypeInformation<TestPojo> testPojoTypeInfo = TypeInformation.of(TestPojo.class);
		ExecutionConfig config = new ExecutionConfig();
		config.disableCodeGeneration();
		TypeSerializer<TestPojo> originalSerializer = testPojoTypeInfo.createSerializer(config);
		System.out.println(originalSerializer.getClass().getCanonicalName());
		config.enableCodeGeneration();
		TypeSerializer<TestPojo> generatedSerializer = testPojoTypeInfo.createSerializer(config);
		System.out.println(generatedSerializer.getClass().getCanonicalName());

		ArrayList<MemorySegment> segments = getMemory(2500, SEGMENT_SIZE);
		ListMemorySegmentSource segmentSource = new ListMemorySegmentSource(segments);
		SimpleCollectingOutputView outputView = new SimpleCollectingOutputView(segments, segmentSource, SEGMENT_SIZE);
		RandomAccessInputView inputView = new RandomAccessInputView(segments, SEGMENT_SIZE);
		TestDataInputView mockedInput = new TestDataInputView();
		TestDataOutputView mockedOutput = new TestDataOutputView(5);
		TestPojo obj = new TestPojo();
		TestPojo reuse = new TestPojo();
		for (int j = 0; j < 10000; ++j) {
			obj.key = 0;
			obj.value = obj.key*2;
			testPerformance(obj, reuse, mockedOutput, mockedInput, originalSerializer, 100);
		}
		long start = System.currentTimeMillis();
		for (int j = 0; j < 100; ++j) {
			obj.key = 0;
			obj.value = obj.key*2;
			testPerformance(obj, reuse, mockedOutput, mockedInput, originalSerializer, 1000000);
		}
		long end = System.currentTimeMillis();
		System.out.println("### Total time: " + (end - start) + "");
	}

	static void testPerformance(TestPojo obj, TestPojo reuse, DataOutputView outputView, DataInputView inputView,
	                     TypeSerializer<TestPojo> serializer, int num) throws IOException {
		for (int i = 0; i < num; ++i) {
			serializer.serialize(obj, outputView);
			obj = serializer.deserialize(inputView);
			obj.key++;
			obj.value = obj.key*2;
		}
	}
}
