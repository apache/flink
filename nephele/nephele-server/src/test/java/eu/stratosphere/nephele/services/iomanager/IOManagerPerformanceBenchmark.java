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

package eu.stratosphere.nephele.services.iomanager;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.services.iomanager.BlockChannelReader;
import eu.stratosphere.nephele.services.iomanager.BlockChannelWriter;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.ChannelReaderInputView;
import eu.stratosphere.nephele.services.iomanager.ChannelWriterOutputView;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.DefaultMemoryManagerTest;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.types.IntegerRecord;

/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class IOManagerPerformanceBenchmark
{
	private static final Log LOG = LogFactory.getLog(IOManagerPerformanceBenchmark.class);
	
	private static final int[] SEGMENT_SIZES_ALIGNED = { 4096, 16384, 524288 };
	
	private static final int[] SEGMENT_SIZES_UNALIGNED = { 3862, 16895, 500481 };
	
	private static final int[] NUM_SEGMENTS = { 1, 2, 4, 6 };
	
	private static final long MEMORY_SIZE = 32 * 1024 * 1024;
	
	private static final int NUM_INTS_WRITTEN = 100000000;
	
	
	private static final AbstractInvokable memoryOwner = new DefaultMemoryManagerTest.DummyInvokable();
	
	private DefaultMemoryManager memManager;
	
	private IOManager ioManager;
	
	
	@Before
	public void startup()
	{
		memManager = new DefaultMemoryManager(MEMORY_SIZE);
		ioManager = new IOManager();
	}
	
	@After
	public void afterTest() throws Exception {
		ioManager.shutdown();
		Assert.assertTrue("IO Manager has not properly shut down.", ioManager.isProperlyShutDown());
		
		Assert.assertTrue("Not all memory was returned to the memory manager in the test.", memManager.verifyEmpty());
		memManager.shutdown();
		memManager = null;
	}
	
// ------------------------------------------------------------------------
	
	@Test
	public void speedTestIOManager() throws Exception
	{
		LOG.info("Starting speed test with IO Manager...");
		
		for (int num : NUM_SEGMENTS) {
			testChannelWithSegments(num);
		}
	}

	private final void testChannelWithSegments(int numSegments) throws Exception
	{
		final List<MemorySegment> memory = this.memManager.allocatePages(memoryOwner, numSegments);
		final Channel.ID channel = this.ioManager.createChannel();
		
		BlockChannelWriter writer = null;
		BlockChannelReader reader = null;
		
		try {	
			writer = this.ioManager.createBlockChannelWriter(channel);
			final ChannelWriterOutputView out = new ChannelWriterOutputView(writer, memory, this.memManager.getPageSize());
			
			long writeStart = System.currentTimeMillis();
			
			int valsLeft = NUM_INTS_WRITTEN;
			while (valsLeft-- > 0) {
				out.writeInt(valsLeft);
			}
			
			out.close();
			final int numBlocks = out.getBlockCount();
			writer.close();
			writer = null;
			
			long writeElapsed = System.currentTimeMillis() - writeStart;
			
			// ----------------------------------------------------------------
			
			reader = ioManager.createBlockChannelReader(channel);
			final ChannelReaderInputView in = new ChannelReaderInputView(reader, memory, numBlocks, false);
			
			long readStart = System.currentTimeMillis();
			
			valsLeft = NUM_INTS_WRITTEN;
			while (valsLeft-- > 0) {
				in.readInt();
//				Assert.assertTrue(rec.getValue() == valsLeft);
			}
			
			in.close();
			reader.close();
			
			long readElapsed = System.currentTimeMillis() - readStart;
			
			reader.deleteChannel();
			reader = null;
			
			LOG.info("IOManager with " + numSegments + " mem segments: write " + writeElapsed + " msecs, read " + readElapsed + " msecs.");
			
			memManager.release(memory);
		}
		finally {
			if (reader != null) {
				reader.closeAndDelete();
			}
			if (writer != null) {
				writer.closeAndDelete();
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

	@Test
	public void speedTestFileStream() throws Exception
	{
		LOG.info("Starting speed test with java io file stream and ALIGNED buffer sizes ...");
		
		for (int bufferSize : SEGMENT_SIZES_ALIGNED)
		{
			speedTestStream(bufferSize);
		}
		
		LOG.info("Starting speed test with java io file stream and UNALIGNED buffer sizes ...");
		
		for (int bufferSize : SEGMENT_SIZES_UNALIGNED)
		{
			speedTestStream(bufferSize);
		}
		
	}
		
	private final void speedTestStream(int bufferSize) throws IOException {
		final Channel.ID tmpChannel = ioManager.createChannel();
		final IntegerRecord rec = new IntegerRecord(0);
		
		File tempFile = null;
		DataOutputStream daos = null;
		DataInputStream dais = null;
		
		try {
			tempFile = new File(tmpChannel.getPath());
			
			FileOutputStream fos = new FileOutputStream(tempFile);
			daos = new DataOutputStream(new BufferedOutputStream(fos, bufferSize));
			
			long writeStart = System.currentTimeMillis();
			
			int valsLeft = NUM_INTS_WRITTEN;
			while (valsLeft-- > 0) {
				rec.setValue(valsLeft);
				rec.write(daos);
			}
			daos.close();
			daos = null;
			
			long writeElapsed = System.currentTimeMillis() - writeStart;
			
			// ----------------------------------------------------------------
			
			FileInputStream fis = new FileInputStream(tempFile);
			dais = new DataInputStream(new BufferedInputStream(fis, bufferSize));
			
			long readStart = System.currentTimeMillis();
			
			valsLeft = NUM_INTS_WRITTEN;
			while (valsLeft-- > 0) {
				rec.read(dais);
			}
			dais.close();
			dais = null;
			
			long readElapsed = System.currentTimeMillis() - readStart;
			
			LOG.info("File-Stream with buffer " + bufferSize + ": write " + writeElapsed + " msecs, read " + readElapsed + " msecs.");
		}
		finally {
			// close if possible
			if (daos != null) {
				daos.close();
			}
			if (dais != null) {
				dais.close();
			}
			// try to delete the file
			if (tempFile != null) {
				tempFile.delete();
			}
		}
	}
	
	// ------------------------------------------------------------------------
	
	@Test
	public void speedTestNIO() throws Exception
	{
		LOG.info("Starting speed test with java NIO heap buffers and ALIGNED buffer sizes ...");
		
		for (int bufferSize : SEGMENT_SIZES_ALIGNED)
		{
			speedTestNIO(bufferSize, false);
		}
		
		LOG.info("Starting speed test with java NIO heap buffers and UNALIGNED buffer sizes ...");
		
		for (int bufferSize : SEGMENT_SIZES_UNALIGNED)
		{
			speedTestNIO(bufferSize, false);
		}
		
		LOG.info("Starting speed test with java NIO direct buffers and ALIGNED buffer sizes ...");
		
		for (int bufferSize : SEGMENT_SIZES_ALIGNED)
		{
			speedTestNIO(bufferSize, true);
		}
		
		LOG.info("Starting speed test with java NIO direct buffers and UNALIGNED buffer sizes ...");
		
		for (int bufferSize : SEGMENT_SIZES_UNALIGNED)
		{
			speedTestNIO(bufferSize, true);
		}
		
	}
		
	private final void speedTestNIO(int bufferSize, boolean direct) throws IOException
	{
		final Channel.ID tmpChannel = ioManager.createChannel();
		
		File tempFile = null;
		FileChannel fs = null;
		
		try {
			tempFile = new File(tmpChannel.getPath());
			
			RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
			fs = raf.getChannel();
			
			ByteBuffer buf = direct ? ByteBuffer.allocateDirect(bufferSize) : ByteBuffer.allocate(bufferSize);
			
			long writeStart = System.currentTimeMillis();
			
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
			fs = null;
			
			long writeElapsed = System.currentTimeMillis() - writeStart;
			
			// ----------------------------------------------------------------
			
			raf = new RandomAccessFile(tempFile, "r");
			fs = raf.getChannel();
			buf.clear();
			
			long readStart = System.currentTimeMillis();
			
			fs.read(buf);
			buf.flip();
			
			valsLeft = NUM_INTS_WRITTEN;
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
			fs = null;

			
			long readElapsed = System.currentTimeMillis() - readStart;
			
			LOG.info("NIO Channel with buffer " + bufferSize + ": write " + writeElapsed + " msecs, read " + readElapsed + " msecs.");
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

}
