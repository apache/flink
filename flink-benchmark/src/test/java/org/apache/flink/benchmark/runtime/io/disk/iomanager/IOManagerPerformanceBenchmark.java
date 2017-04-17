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

package org.apache.flink.benchmark.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;
import org.apache.flink.runtime.io.disk.iomanager.*;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.types.IntValue;
import org.junit.Assert;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class IOManagerPerformanceBenchmark {

	private static final Logger LOG = LoggerFactory.getLogger(IOManagerPerformanceBenchmark.class);

	@Param({"4096", "16384", "524288"})
	private int segmentSizesAligned;

	@Param({"3862", "16895", "500481"})
	private int segmentSizesUnaligned;

	@Param({"1", "2", "4", "6"})
	private int numSegment;

	private static int numBlocks;

	private static final long MEMORY_SIZE = 32 * 1024 * 1024;

	private static final int NUM_INTS_WRITTEN = 100000000;

	private static final AbstractInvokable memoryOwner = new DummyInvokable();

	private MemoryManager memManager;

	private IOManager ioManager;

	private static FileIOChannel.ID fileIOChannel;

	private static File ioManagerTempFile1;

	private static File ioManagerTempFile2;

	private static File speedTestNIOTempFile1;

	private static File speedTestNIOTempFile2;

	private static File speedTestNIOTempFile3;

	private static File speedTestNIOTempFile4;


	@Setup
	public void startup() throws Exception {
		memManager = new MemoryManager(MEMORY_SIZE, 1);
		ioManager = new IOManagerAsync();
		testChannelWriteWithSegments(numSegment);
		ioManagerTempFile1 = createReadTempFile(segmentSizesAligned);
		ioManagerTempFile2 = createReadTempFile(segmentSizesUnaligned);
		speedTestNIOTempFile1 = createSpeedTestNIOTempFile(segmentSizesAligned, true);
		speedTestNIOTempFile2 = createSpeedTestNIOTempFile(segmentSizesAligned, false);
		speedTestNIOTempFile3 = createSpeedTestNIOTempFile(segmentSizesUnaligned, true);
		speedTestNIOTempFile4 = createSpeedTestNIOTempFile(segmentSizesUnaligned, false);
	}

	@TearDown
	public void afterTest() throws Exception {
		ioManagerTempFile1.delete();
		ioManagerTempFile2.delete();
		speedTestNIOTempFile1.delete();
		speedTestNIOTempFile2.delete();
		speedTestNIOTempFile3.delete();
		speedTestNIOTempFile4.delete();
		ioManager.shutdown();
		Assert.assertTrue("IO Manager has not properly shut down.", ioManager.isProperlyShutDown());

		Assert.assertTrue("Not all memory was returned to the memory manager in the test.", memManager.verifyEmpty());
		memManager.shutdown();
		memManager = null;
	}

// ------------------------------------------------------------------------

	private File createReadTempFile(int bufferSize) throws IOException {
		final FileIOChannel.ID tmpChannel = ioManager.createChannel();
		final IntValue rec = new IntValue(0);

		File tempFile = null;
		DataOutputStream daos = null;

		try {
			tempFile = new File(tmpChannel.getPath());

			FileOutputStream fos = new FileOutputStream(tempFile);
			daos = new DataOutputStream(new BufferedOutputStream(fos, bufferSize));

			int valsLeft = NUM_INTS_WRITTEN;
			while (valsLeft-- > 0) {
				rec.setValue(valsLeft);
				rec.write(new OutputViewDataOutputStreamWrapper(daos));
			}
			daos.close();
			daos = null;
		}
		finally {
			// close if possible
			if (daos != null) {
				daos.close();
			}
		}
		return tempFile;
	}

	@SuppressWarnings("resource")
	private File createSpeedTestNIOTempFile(int bufferSize, boolean direct) throws IOException
	{
		final FileIOChannel.ID tmpChannel = ioManager.createChannel();

		File tempFile = null;
		FileChannel fs = null;

		try {
			tempFile = new File(tmpChannel.getPath());

			RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
			fs = raf.getChannel();

			ByteBuffer buf = direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);

			int valsLeft = NUM_INTS_WRITTEN;
			while (valsLeft-- > 0) {
				if (buf.remaining() < 4) {
					buf.flip();
					fs.write(buf);
					buf.clear();
				}
				buf.putInt(valsLeft);
			}

			if (buf.position() > 0) {
				buf.flip();
				fs.write(buf);
			}

			fs.close();
			raf.close();
			fs = null;
		}
		finally {
			// close if possible
			if (fs != null) {
				fs.close();
				fs = null;
			}
		}
		return tempFile;
	}

	@Benchmark
	public void speedTestOutputManager() throws Exception
	{
		LOG.info("Starting speed test with IO Manager...");

		testChannelWriteWithSegments(numSegment);
	}

	@Benchmark
	public void speedTestInputManager() throws Exception
	{
		LOG.info("Starting speed test with IO Manager...");

		testChannelReadWithSegments(numSegment);
	}

	private void testChannelWriteWithSegments(int numSegments) throws Exception
	{
		final List<MemorySegment> memory = this.memManager.allocatePages(memoryOwner, numSegments);
		final FileIOChannel.ID channel = this.ioManager.createChannel();

		BlockChannelWriter<MemorySegment> writer = null;

		try {
			writer = this.ioManager.createBlockChannelWriter(channel);
			final ChannelWriterOutputView out = new ChannelWriterOutputView(writer, memory, this.memManager.getPageSize());

			int valsLeft = NUM_INTS_WRITTEN;
			while (valsLeft-- > 0) {
				out.writeInt(valsLeft);
			}

			fileIOChannel = channel;
			out.close();
			numBlocks = out.getBlockCount();

			writer.close();
			writer = null;

			memManager.release(memory);
		}
		finally {
			if (writer != null) {
				writer.closeAndDelete();
			}
		}
	}

	private void testChannelReadWithSegments(int numSegments) throws Exception
	{
		final List<MemorySegment> memory = this.memManager.allocatePages(memoryOwner, numSegments);

		BlockChannelReader<MemorySegment> reader = null;

		try {
			reader = ioManager.createBlockChannelReader(fileIOChannel);
			final ChannelReaderInputView in = new ChannelReaderInputView(reader, memory, numBlocks, false);

			int valsLeft = NUM_INTS_WRITTEN;
			while (valsLeft-- > 0) {
				in.readInt();
//				Assert.assertTrue(rec.getValue() == valsLeft);
			}

			in.close();
			reader.close();
			reader = null;

			memManager.release(memory);
		}
		finally {
			if (reader != null) {
				reader.closeAndDelete();
			}
		}
	}

//	@Test
//	public void speedTestRandomAccessFile() throws IOException {
//		LOG.info("Starting speed test with java random access file ...");
//
//		Channel.ID tmpChannel = ioManager.createChannel();
//		File tempFile = null;
//		RandomAccessFile raf = null;
//
//		try {
//			tempFile = new File(tmpChannel.getPath());
//			raf = new RandomAccessFile(tempFile, "rw");
//
//			IntegerRecord rec = new IntegerRecord(0);
//
//			long writeStart = System.currentTimeMillis();
//
//			int valsLeft = NUM_INTS_WRITTEN;
//			while (valsLeft-- > 0) {
//				rec.setValue(valsLeft);
//				rec.write(raf);
//			}
//			raf.close();
//			raf = null;
//
//			long writeElapsed = System.currentTimeMillis() - writeStart;
//
//			// ----------------------------------------------------------------
//
//			raf = new RandomAccessFile(tempFile, "r");
//
//			long readStart = System.currentTimeMillis();
//
//			valsLeft = NUM_INTS_WRITTEN;
//			while (valsLeft-- > 0) {
//				rec.read(raf);
//			}
//			raf.close();
//			raf = null;
//
//			long readElapsed = System.currentTimeMillis() - readStart;
//
//
//			LOG.info("Random Access File: write " + (writeElapsed / 1000) + " secs, read " + (readElapsed / 1000) + " secs.");
//		}
//		finally {
//			// close if possible
//			if (raf != null) {
//				raf.close();
//			}
//
//			// try to delete the file
//			if (tempFile != null) {
//				tempFile.delete();
//			}
//		}
//	}

	@Benchmark
	public void speedOutputStreamWithBufferAligned() throws Exception
	{
		LOG.info("Starting speed test with java io file stream and ALIGNED buffer sizes ...");

		speedOutputTestStream(segmentSizesAligned);
	}

	@Benchmark
	public void speedOutputStreamWithBufferUnaligned() throws Exception
	{
		LOG.info("Starting speed test with java io file stream and UNALIGNED buffer sizes ...");

		speedOutputTestStream(segmentSizesUnaligned);
	}

	@Benchmark
	public void speedInputStreamWithBufferAligned() throws Exception
	{
		LOG.info("Starting speed test with java io file stream and ALIGNED buffer sizes ...");

		speedInputTestStream(segmentSizesAligned);
	}

	@Benchmark
	public void speedInputStreamWithBufferUnaligned() throws Exception
	{
		LOG.info("Starting speed test with java io file stream and UNALIGNED buffer sizes ...");

		speedInputTestStream(segmentSizesUnaligned);
	}

	private void speedOutputTestStream(int bufferSize) throws IOException {
		final FileIOChannel.ID tmpChannel = ioManager.createChannel();
		final IntValue rec = new IntValue(0);

		File tempFile = null;
		DataOutputStream daos = null;

		try {
			tempFile = new File(tmpChannel.getPath());

			FileOutputStream fos = new FileOutputStream(tempFile);
			daos = new DataOutputStream(new BufferedOutputStream(fos, bufferSize));

			int valsLeft = NUM_INTS_WRITTEN;
			while (valsLeft-- > 0) {
				rec.setValue(valsLeft);
				rec.write(new OutputViewDataOutputStreamWrapper(daos));
			}
			daos.close();
			daos = null;
		}
		finally {
			// close if possible
			if (daos != null) {
				daos.close();
			}
			// try to delete the file
			if (tempFile != null) {
				tempFile.delete();
			}
		}
	}

	private void speedInputTestStream(int bufferSize) throws IOException {
		final FileIOChannel.ID tmpChannel = ioManager.createChannel();
		final IntValue rec = new IntValue(0);

		File tempFile = null;
		DataInputStream dais = null;

		if (((bufferSize == 4096)||(bufferSize == 16384)||(bufferSize == 524288)))
		{
			tempFile = ioManagerTempFile1;
		}
		if (((bufferSize == 3862)||(bufferSize == 16895)||(bufferSize == 500481)))
		{
			tempFile = ioManagerTempFile2;
		}

		try {
			FileInputStream fis = new FileInputStream(tempFile);
			dais = new DataInputStream(new BufferedInputStream(fis, bufferSize));

			int valsLeft = NUM_INTS_WRITTEN;
			while (valsLeft-- > 0) {
				rec.read(new InputViewDataInputStreamWrapper(dais));
			}
			dais.close();
			dais = null;
		}
		finally {
			// close if possible
			if (dais != null) {
				dais.close();
			}
		}
	}

	// ------------------------------------------------------------------------

	@Benchmark
	public void speedWriteIndirectAndBufferAligned() throws Exception
	{
		LOG.info("Starting speed test with java NIO heap buffers and ALIGNED buffer sizes ...");

		speedWriteTestNIO(segmentSizesAligned, false);
	}

	@Benchmark
	public void speedWriteIndirectAndBufferUnaligned() throws Exception
	{
		LOG.info("Starting speed test with java NIO heap buffers and UNALIGNED buffer sizes ...");

		speedWriteTestNIO(segmentSizesUnaligned, false);
	}

	@Benchmark
	public void speedWriteDirectAndBufferAligned() throws Exception
	{
		LOG.info("Starting speed test with java NIO direct buffers and ALIGNED buffer sizes ...");

		speedWriteTestNIO(segmentSizesAligned, true);
	}

	@Benchmark
	public void speedWriteDirectAndBufferUnaligned() throws Exception
	{
		LOG.info("Starting speed test with java NIO direct buffers and UNALIGNED buffer sizes ...");

		speedWriteTestNIO(segmentSizesUnaligned, true);
	}

	@Benchmark
	public void speedReadIndirectAndBufferAligned() throws Exception
	{
		LOG.info("Starting speed test with java NIO heap buffers and ALIGNED buffer sizes ...");

		speedReadTestNIO(segmentSizesAligned, false);
	}

	@Benchmark
	public void speedReadIndirectAndBufferUnaligned() throws Exception
	{
		LOG.info("Starting speed test with java NIO heap buffers and UNALIGNED buffer sizes ...");

		speedReadTestNIO(segmentSizesUnaligned, false);
	}

	@Benchmark
	public void speedReadDirectAndBufferAligned() throws Exception
	{
		LOG.info("Starting speed test with java NIO direct buffers and ALIGNED buffer sizes ...");

		speedReadTestNIO(segmentSizesAligned, true);
	}

	@Benchmark
	public void speedReadDirectAndBufferUnaligned() throws Exception
	{
		LOG.info("Starting speed test with java NIO direct buffers and UNALIGNED buffer sizes ...");

		speedReadTestNIO(segmentSizesUnaligned, true);
	}


	@SuppressWarnings("resource")
	private void speedWriteTestNIO(int bufferSize, boolean direct) throws IOException
	{
		final FileIOChannel.ID tmpChannel = ioManager.createChannel();

		File tempFile = null;
		FileChannel fs = null;

		try {
			tempFile = new File(tmpChannel.getPath());

			RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
			fs = raf.getChannel();

			ByteBuffer buf = direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);

			int valsLeft = NUM_INTS_WRITTEN;
			while (valsLeft-- > 0) {
				if (buf.remaining() < 4) {
					buf.flip();
					fs.write(buf);
					buf.clear();
				}
				buf.putInt(valsLeft);
			}

			if (buf.position() > 0) {
				buf.flip();
				fs.write(buf);
			}

			fs.close();
			raf.close();
			fs = null;
		}
		finally {
			// close if possible
			if (fs != null) {
				fs.close();
				fs = null;
			}
			// try to delete the file
			if (tempFile != null) {
				tempFile.delete();
			}
		}
	}

	@SuppressWarnings("resource")
	private void speedReadTestNIO(int bufferSize, boolean direct) throws IOException
	{
		File tempFile = null;
		FileChannel fs = null;

		if (((bufferSize == 4096)||(bufferSize == 16384)||(bufferSize == 524288))&&(direct))
		{
			tempFile = speedTestNIOTempFile1;
		}
		if (((bufferSize == 4096)||(bufferSize == 16384)||(bufferSize == 524288))&&(!direct))
		{
			tempFile = speedTestNIOTempFile2;
		}
		if (((bufferSize == 3862)||(bufferSize == 16895)||(bufferSize == 500481))&&(direct))
		{
			tempFile = speedTestNIOTempFile3;
		}
		if (((bufferSize == 3862)||(bufferSize == 16895)||(bufferSize == 500481))&&(!direct))
		{
			tempFile = speedTestNIOTempFile4;
		}

		try {
			ByteBuffer buf = direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);

			RandomAccessFile raf = new RandomAccessFile(tempFile, "r");
			fs = raf.getChannel();
			buf.clear();

			fs.read(buf);
			buf.flip();

			int valsLeft = NUM_INTS_WRITTEN;
			while (valsLeft-- > 0) {
				if (buf.remaining() < 4) {
					buf.compact();
					fs.read(buf);
					buf.flip();
				}
				if (buf.getInt() != valsLeft) {
					throw new IOException();
				}
			}

			fs.close();
			raf.close();

		}
		finally {
			// close if possible
			if (fs != null) {
				fs.close();
				fs = null;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Options opt = new OptionsBuilder()
				.include(IOManagerPerformanceBenchmark.class.getSimpleName())
				.warmupIterations(2)
				.measurementIterations(2)
				.forks(1)
				.build();
		new Runner(opt).run();
	}
}
