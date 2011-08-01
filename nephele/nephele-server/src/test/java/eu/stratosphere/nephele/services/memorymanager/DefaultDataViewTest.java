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

package eu.stratosphere.nephele.services.memorymanager;

import java.util.Random;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;

public class DefaultDataViewTest
{

	public static final long RANDOM_SEED = 643196033469871L;
	
	private final AbstractInvokable mockInvoke = new DummyInvokable();
	
	private final int MEMORY_SIZE = 1024;
	
	private DefaultMemoryManager memoryManager;

	@Before
	public void setUp() {
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
	}

	@After
	public void tearDown() throws Exception {
		if (!memoryManager.verifyEmpty()) {
			Assert.fail("Memory manager is not complete empty and valid at the end of the test.");
		}
		
		memoryManager = null;
	}

	@Test
	public void testWriteReadInt() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < (MEMORY_SIZE / 4); i++) {
			segment.outputView.writeInt(rand.nextInt());
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < (MEMORY_SIZE / 4); i++) {
			if(segment.inputView.readInt() != rand.nextInt()) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteReadLong() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < (MEMORY_SIZE / 8); i++) {
			segment.outputView.writeLong(rand.nextLong());
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < (MEMORY_SIZE / 8); i++) {
			if(segment.inputView.readLong() != rand.nextLong()) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteReadFloat() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < (MEMORY_SIZE / 4); i++) {
			segment.outputView.writeFloat(rand.nextFloat());
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < (MEMORY_SIZE / 4); i++) {
			if(segment.inputView.readFloat() != rand.nextFloat()) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteReadDouble() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < (MEMORY_SIZE / 8); i++) {
			segment.outputView.writeDouble(rand.nextDouble());
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < (MEMORY_SIZE / 8); i++) {
			if(segment.inputView.readDouble() != rand.nextDouble()) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteReadShort() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < (MEMORY_SIZE / 2); i++) {
			segment.outputView.writeShort(rand.nextInt());
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < (MEMORY_SIZE / 2); i++) {
			if(segment.inputView.readShort() != (short)(rand.nextInt() & 0xffff)) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteReadBoolean() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < MEMORY_SIZE; i++) {
			segment.outputView.writeBoolean(rand.nextBoolean());
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < MEMORY_SIZE; i++) {
			if(segment.inputView.readBoolean() != rand.nextBoolean()) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteReadChar() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < (MEMORY_SIZE / 2); i++) {
			segment.outputView.writeChar((char) (rand.nextInt() & 0xffff));
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < (MEMORY_SIZE / 2); i++) {
			if(segment.inputView.readChar() != ((char) (rand.nextInt() & 0xffff))) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteReadByte() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < MEMORY_SIZE; i++) {
			segment.outputView.writeByte(rand.nextInt());
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < MEMORY_SIZE; i++) {
			if(segment.inputView.readByte() != (byte)(rand.nextInt() & 0xff)) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteReadUTF() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);

		String str = generateRandomString(rand, 32);
		int lengthSum = str.length()*3;
		int writeCnt = 0;
				
		while(lengthSum < MEMORY_SIZE) {
			segment.outputView.writeUTF(str);
			writeCnt++;
			str = generateRandomString(rand, 32);
			lengthSum += str.length()*3;
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < writeCnt; i++) {
			if(!segment.inputView.readUTF().equals(generateRandomString(rand, 32))) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteSingleByte() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < MEMORY_SIZE; i++) {
			segment.outputView.write(rand.nextInt());
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < MEMORY_SIZE; i++) {
			if(segment.inputView.readByte() != (byte)(rand.nextInt() & 0xff)) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteByteArray() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);

		byte[] arr = new byte[0];
		int lengthSum = 0;
				
		while(lengthSum < MEMORY_SIZE) {
			segment.outputView.write(arr);			
			arr = generateRandomByteArray(rand, 16);
			lengthSum += arr.length;
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < lengthSum; i++) {
			arr = generateRandomByteArray(rand, 16);
			for(int j=0; j < arr.length; j++) {
				if(segment.inputView.readByte() != arr[j]) {
					Assert.fail();
				}
			}
			i += arr.length;
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteSubByteArray() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);

		byte[] arr = generateRandomByteArray(rand, 24);
		int lengthSum = arr.length;
				
		while(lengthSum < MEMORY_SIZE) {
			int off = rand.nextInt(arr.length);
			int len = rand.nextInt(arr.length - off);
			segment.outputView.write(arr, off, len);			
			arr = generateRandomByteArray(rand, 24);
			lengthSum += arr.length;
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < lengthSum; i++) {
			arr = generateRandomByteArray(rand, 24);
			int off = rand.nextInt(arr.length);
			int len = rand.nextInt(arr.length - off);
			for(int j=off; j < off+len; j++) {
				if(segment.inputView.readByte() != arr[j]) {
					Assert.fail();
				}
			}
			i += arr.length;
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteBytes() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);

		String str = generateRandomString(rand, 32);
		int lengthSum = str.length();
				
		while(lengthSum < MEMORY_SIZE) {
			segment.outputView.writeBytes(str);
			str = generateRandomString(rand, 32);
			lengthSum += str.length();
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < lengthSum; i++) {
			str = generateRandomString(rand, 32);
			for(int j=0; j < str.length(); j++) {
				char c = str.charAt(j);
				if(segment.inputView.readByte() != (byte)(c & 0xff)) {
					Assert.fail();
				}
			}
			i += str.length();
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testWriteChars() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);

		String str = generateRandomString(rand, 32);
		int lengthSum = str.length()*2;
				
		while(lengthSum < MEMORY_SIZE) {
			segment.outputView.writeChars(str);
			str = generateRandomString(rand, 32);
			lengthSum += str.length()*2;
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < lengthSum/2; i++) {
			str = generateRandomString(rand, 32);
			for(int j=0; j < str.length(); j++) {
				char c = str.charAt(j);
				if(segment.inputView.readChar() != c) {
					Assert.fail();
				}
			}
			i += str.length();
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testReadFully() throws Exception { 
		Random rand = new Random(RANDOM_SEED);
		Random rand2 = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < MEMORY_SIZE; i++) {
			segment.outputView.writeByte(rand.nextInt());
		}
		
		rand.setSeed(RANDOM_SEED);
		byte[] arr = generateRandomByteArray(rand2, 32);
		int lengthSum = arr.length;
		
		while(lengthSum < MEMORY_SIZE) {
			segment.inputView.readFully(arr);
			for(int j=0; j<arr.length; j++) {
				if(arr[j] != (byte)(rand.nextInt() & 0xff)) {
					Assert.fail();
				}
			}
			arr = generateRandomByteArray(rand2, 32);
			lengthSum += arr.length;
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testReadFullyOffset() throws Exception { 
		
		Random rand = new Random(RANDOM_SEED);
		Random rand2 = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < MEMORY_SIZE; i++) {
			segment.outputView.writeByte(rand.nextInt());
		}
		
		rand.setSeed(RANDOM_SEED);
		byte[] arr = generateRandomByteArray(rand2, 32);
		int lengthSum = arr.length;
		int off = rand2.nextInt(arr.length);
		int len = rand2.nextInt(arr.length-off);
		
		while(lengthSum < MEMORY_SIZE) {
			segment.inputView.readFully(arr, off, len);
			for(int j=off; j<off+len; j++) {
				if(arr[j] != (byte)(rand.nextInt() & 0xff)) {
					Assert.fail();
				}
			}
			arr = generateRandomByteArray(rand2, 32);
			off = rand2.nextInt(arr.length);
			len = rand2.nextInt(arr.length-off);
			lengthSum += arr.length;
		}
		
		memoryManager.release(segment);
		
	}
	
	@Test
	public void testReadLine() throws Exception { 
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);

		String str = generateRandomString(rand, 32);
		str = str.replaceAll("\n", "_").replaceAll("\r", "-")+"\n";
		int lengthSum = str.length()*2;
		int writeCnt = 0;
				
		while(lengthSum < MEMORY_SIZE) {
			segment.outputView.writeChars(str);
			writeCnt++;
			str = generateRandomString(rand, 32);
			str = str.replaceAll("\n", "_").replaceAll("\r", "-")+"\n";
			lengthSum += str.length()*2;
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < writeCnt; i++) {
			str = generateRandomString(rand, 32);
			str = str.replaceAll("\n", "_").replaceAll("\r", "-");
			if(!segment.inputView.readLine().equals(str)) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
		
	}
	
	@Test
	public void testSkipBytes() throws Exception { 
		
		Random rand = new Random(RANDOM_SEED);
		Random rand2 = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < MEMORY_SIZE; i++) {
			segment.outputView.writeByte(rand.nextInt());
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < MEMORY_SIZE; i++) {

			if(segment.inputView.readByte() != (byte)(rand.nextInt() & 0xff)) {
				Assert.fail();
			}
			int skipCnt = rand2.nextInt(16);
			segment.inputView.skipBytes(skipCnt);
			for(int j=0; j<skipCnt; j++) {
				rand.nextInt();
			}
			i += skipCnt;
		}
		
		memoryManager.release(segment);
		
	}

	@Test
	public void testReadUnsignedShort() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < (MEMORY_SIZE / 2); i++) {
			segment.outputView.writeShort(rand.nextInt());
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < (MEMORY_SIZE / 2); i++) {
			int uSh = rand.nextInt() & 0xffff;
			uSh = (uSh & 0xff00) | (uSh & 0xff);
			if(segment.inputView.readUnsignedShort() != uSh) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testReadUnsignedByte() throws Exception {
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);		

		for(int i=0; i < MEMORY_SIZE; i++) {
			segment.outputView.writeByte(rand.nextInt());
		}
		
		rand.setSeed(RANDOM_SEED);
		for(int i=0; i < MEMORY_SIZE; i++) {
			if(segment.inputView.readUnsignedByte() != (int)(rand.nextInt() & 0xff)) {
				Assert.fail();
			}
		}
		
		memoryManager.release(segment);
	}
	
	@Test
	public void testReadWriteMixed() throws Exception { 
		
		Random rand = new Random(RANDOM_SEED);
		MemorySegment segment = memoryManager.allocate(mockInvoke, MEMORY_SIZE);
		
		segment.outputView.writeByte(rand.nextInt());
		segment.outputView.writeDouble(rand.nextDouble());
		segment.outputView.writeBoolean(rand.nextBoolean());
		segment.outputView.write(rand.nextInt());
		segment.outputView.writeChar('c');
		segment.outputView.writeChars("abcdef\n");
		segment.outputView.writeFloat(rand.nextFloat());
		segment.outputView.writeShort(rand.nextInt());
		segment.outputView.writeByte(rand.nextInt());
		
		rand.setSeed(RANDOM_SEED);
		if(segment.inputView.readByte() != (byte)(rand.nextInt() & 0xff)) Assert.fail();
		if(segment.inputView.readDouble() != rand.nextDouble()) Assert.fail();
		if(segment.inputView.readBoolean() != rand.nextBoolean()) Assert.fail();
		if(segment.inputView.readByte() != (byte)(rand.nextInt() & 0xff)) Assert.fail();
		if(segment.inputView.readChar() != 'c') Assert.fail();
		if(!segment.inputView.readLine().equals("abcdef")) Assert.fail();
		if(segment.inputView.readFloat() != rand.nextFloat()) Assert.fail();
		if(segment.inputView.readShort() != (short)(rand.nextInt() & 0xffff)) Assert.fail();
		if(segment.inputView.readByte() != (byte)(rand.nextInt() & 0xff)) Assert.fail();
		
		memoryManager.release(segment);
		
	}
	
	private byte[] generateRandomByteArray(Random rand, int maxLength) {
		
		int length = 1+rand.nextInt(maxLength-1);
		byte[] arr = new byte[length];
		rand.nextBytes(arr);
		
		return arr;
	}
	
	private String generateRandomString(Random rand, int maxLength) {
		
		return new String(generateRandomByteArray(rand, maxLength));
	}
	
	
	/**
	 * Utility class to serve as owner for the memory.
	 */
	public static final class DummyInvokable extends AbstractInvokable {
		@Override
		public void registerInputOutput() {}

		@Override
		public void invoke() throws Exception {}
	}
}
