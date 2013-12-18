/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.channels;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.util.BufferPoolConnector;


public class MemoryBufferTest {

	private MemoryBufferPoolConnector bufferPoolConnector;
	private Queue<MemorySegment> bufferPool;
	
	private final static int INT_COUNT = 512;
	private final static int INT_SIZE = Integer.SIZE / Byte.SIZE;

	@Before
	public void setUp() throws Exception {
		bufferPool = new LinkedBlockingQueue<MemorySegment>();
		bufferPoolConnector = new BufferPoolConnector(bufferPool);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void readToSmallByteBuffer() throws IOException {
		MemoryBuffer buf = new MemoryBuffer(INT_COUNT*INT_SIZE, new MemorySegment(new byte[INT_COUNT*INT_SIZE]), bufferPoolConnector);
		fillBuffer(buf);
		
		ByteBuffer target = ByteBuffer.allocate(INT_SIZE);
		ByteBuffer largeTarget = ByteBuffer.allocate(INT_COUNT*INT_SIZE);
		int i = 0;
		while(buf.hasRemaining()) {
			buf.read(target);
			target.rewind();
			largeTarget.put(target);
			target.rewind();
			if( i++ >= INT_COUNT) {
				fail("There were too many elements in the buffer");
			}
		}
		assertEquals(-1, buf.read(target));
		
		target.rewind();
		validateByteBuffer(largeTarget);
	}
		
	
	/**
	 * CopyToBuffer uses system.arraycopy()
	 * 
	 * @throws IOException
	 */
	@Test
	public void copyToBufferTest() throws IOException {

		MemoryBuffer buf = new MemoryBuffer(INT_COUNT*INT_SIZE, new MemorySegment(new byte[INT_COUNT*INT_SIZE]), bufferPoolConnector);
		fillBuffer(buf);
		
		
		// the target buffer is larger to check if the limit is set appropriately
		MemoryBuffer destination = new MemoryBuffer(INT_COUNT*INT_SIZE*2, 
					new MemorySegment(new byte[INT_COUNT*INT_SIZE*2]), 
					bufferPoolConnector);
		assertEquals(INT_COUNT*INT_SIZE*2, destination.limit());
		// copy buf contents to double sized MemBuffer
		buf.copyToBuffer(destination);
		assertEquals(INT_COUNT*INT_SIZE, destination.limit());
		
		// copy contents of destination to byteBuffer
		ByteBuffer test = ByteBuffer.allocate(INT_COUNT*INT_SIZE);
		int written = destination.read(test);
		assertEquals(INT_COUNT*INT_SIZE, written);
		// validate byteBuffer contents
		validateByteBuffer(test);
		
		destination.position(written);
		destination.limit(destination.getTotalSize());
		// allocate another byte buffer to write the rest of destination into a byteBuffer
		ByteBuffer testRemainder = ByteBuffer.allocate(INT_COUNT*INT_SIZE);
		written = destination.read(testRemainder);
		assertEquals(INT_COUNT*INT_SIZE, written);
		expectAllNullByteBuffer(testRemainder);
		
		buf.close(); // make eclipse happy
	}
	
	@Test
	public void testDuplicate() throws Exception {
		MemoryBuffer buf = new MemoryBuffer(INT_COUNT*INT_SIZE, new MemorySegment(new byte[INT_COUNT*INT_SIZE]), bufferPoolConnector);
		MemoryBuffer buf2 = buf.duplicate();
		
		buf2.close();
		buf.close();
	}

	private void fillBuffer(Buffer buf) throws IOException {
		ByteBuffer src = ByteBuffer.allocate(INT_SIZE);
		// write some data into buf:
		for(int i = 0; i < INT_COUNT; ++i) {
			src.putInt(0,i);
			src.rewind();
			buf.write(src);
		}
		buf.flip();
	}
	
	
	/**
	 * Validates if the ByteBuffer contains the what fillMemoryBuffer has written!
	 * 
	 * @param target
	 */
	private void validateByteBuffer(ByteBuffer target) {
		ByteBuffer ref = ByteBuffer.allocate(INT_SIZE);
		
		for(int i = 0; i < INT_SIZE*INT_COUNT; ++i) {
			ref.putInt(0,i / INT_SIZE);
			assertEquals("Byte at position "+i+" is different", ref.get(i%INT_SIZE), target.get(i));
		}
	}
	
	private void expectAllNullByteBuffer(ByteBuffer target) {
		ByteBuffer ref = ByteBuffer.allocate(INT_SIZE);
		ref.putInt(0,0);
		for(int i = 0; i < INT_COUNT; ++i) {
			assertEquals("Byte at position "+i+" is different", ref.getInt(0), target.getInt(i));
		}
	}
}
