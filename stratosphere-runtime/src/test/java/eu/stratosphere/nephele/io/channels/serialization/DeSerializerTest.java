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

package eu.stratosphere.nephele.io.channels.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.io.channels.DefaultDeserializer;
import eu.stratosphere.nephele.io.channels.SerializationBuffer;


/**
 * @author Stephan Ewen
 */
public class DeSerializerTest
{
	private static final SerializationTestType[] TYPE_FACTORIES = new SerializationTestType[] {
		new BooleanType(),
		new ByteArrayType(),
		new ByteSubArrayType(),
		new ByteType(),
		new CharType(),
		new DoubleType(),
		new FloatType(),
		new IntType(),
		new LongType(),
		new ShortType(),
		new UnsignedByteType(),
		new UnsignedShortType(),
		new UTFStringType()
	};
	
	private static final long SEED = 64871654635745873L;
	
	private Random rnd;
	
	// --------------------------------------------------------------------------------------------
	
	@Before
	public void setup()
	{
		this.rnd = new Random(SEED);
	}

	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testSequenceOfIntegersWithAlignedBuffers()
	{
		try {
			final Random rnd = this.rnd;
			final int NUM_INTS = 1000000;
			
			final Iterator<SerializationTestType> intSource = new Iterator<SerializationTestType>()
			{
				private final Random random = rnd;
				private final int limit = NUM_INTS;
				private int pos = 0;
				
				@Override
				public boolean hasNext() {
					return this.pos < this.limit;
				}
				@Override
				public IntType next() {
					if (hasNext()) {
						this.pos++;
						return new IntType(this.random.nextInt());
					} else {
						throw new NoSuchElementException();
					}
				}
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
			
			testSequenceOfTypes(intSource, 2048);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Test encountered an unexpected exception.");
		}
	}
	
	@Test
	public void testSequenceOfIntegersWithUnalignedBuffers()
	{
		try {
			final Random rnd = this.rnd;
			final int NUM_INTS = 1000000;
			
			final Iterator<SerializationTestType> intSource = new Iterator<SerializationTestType>()
			{
				private final Random random = rnd;
				private final int limit = NUM_INTS;
				private int pos = 0;
				
				@Override
				public boolean hasNext() {
					return this.pos < this.limit;
				}
				@Override
				public IntType next() {
					if (hasNext()) {
						this.pos++;
						return new IntType(this.random.nextInt());
					} else {
						throw new NoSuchElementException();
					}
				}
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
			
			testSequenceOfTypes(intSource, 2047);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Test encountered an unexpected exception.");
		}
	}
	
	@Test
	public void testRandomTypes()
	{
		try {
			final Random rnd = this.rnd;
			final int NUM_TYPES = 1000000;
			
			final Iterator<SerializationTestType> randomSource = new Iterator<SerializationTestType>()
			{
				private final Random random = rnd;
				private final int limit = NUM_TYPES;
				private int pos = 0;
				
				@Override
				public boolean hasNext() {
					return this.pos < this.limit;
				}
				@Override
				public SerializationTestType next() {
					if (hasNext()) {
						this.pos++;
						return TYPE_FACTORIES[this.random.nextInt(TYPE_FACTORIES.length)].getRandom(this.random);
					} else {
						throw new NoSuchElementException();
					}
				}
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
			
			// test with an odd buffer size to force many unaligned cases
			testSequenceOfTypes(randomSource, 512 * 7);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Test encountered an unexpected exception.");
		}
	}
	
	private static final void testSequenceOfTypes(Iterator<SerializationTestType> sequence, int bufferSize) throws Exception
	{
		final ArrayDeque<SerializationTestType> elements = new ArrayDeque<SerializationTestType>(512);
		
		final PipeChannel channel = new PipeChannel(bufferSize);
		final SerializationBuffer<SerializationTestType> serBuffer = new SerializationBuffer<SerializationTestType>();
		final DefaultDeserializer<SerializationTestType> deserBuffer = new DefaultDeserializer<SerializationTestType>(null);
		
		while (sequence.hasNext()) {
			final SerializationTestType type = sequence.next();
			
			// serialize the record
			serBuffer.serialize(type);
			elements.addLast(type);
			
			// write the serialized record
			while (true) {
				serBuffer.read(channel);
				if (serBuffer.dataLeftFromPreviousSerialization()) {
					// current buffer is full, we need to start de-serializing to make space
					channel.flip();
					
					while (!elements.isEmpty()) {
						final SerializationTestType reference = elements.pollFirst();
						final SerializationTestType result = deserBuffer.readData(reference.getClass().newInstance(), channel);
						if (result == null) {
							// not yet complete, we need to break
							elements.addFirst(reference);
							break;
						} else {
							// validate that we deserialized correctly
							assertEquals("The deserialized element is not equal to the serialized element.", reference, result);
						}
					}
					
					channel.clear();
				} else {
					break;
				}
			}
		}
		
		// check the remaining records in the buffers...
		channel.flip();
		while (!elements.isEmpty()) {
			final SerializationTestType reference = elements.pollFirst();
			final SerializationTestType result = deserBuffer.readData(reference.getClass().newInstance(), channel);
			
			assertNotNull(result);
			assertEquals("The deserialized element is not equal to the serialized element.", reference, result);
		}
	}
	
	// ============================================================================================
	
	private static final class PipeChannel implements WritableByteChannel, ReadableByteChannel
	{
		private final byte[] buffer;
		
		private int position;
		private int limit;
		
		
		PipeChannel(int capacity) {
			this.buffer = new byte[capacity];
			this.limit = capacity;
		}
		
		public void flip() {
			this.limit = this.position;
			this.position = 0;
		}
		
		public void clear() {
			this.position = 0;
			this.limit = this.buffer.length;
		}
		
		@Override
		public boolean isOpen() {
			return true;
		}

		@Override
		public void close()
		{}


		@Override
		public int write(ByteBuffer src)
		{
			final int toGet = Math.min(this.limit - this.position, src.remaining());
			src.get(this.buffer, this.position, toGet);
			this.position += toGet;
			return toGet;
		}


		@Override
		public int read(ByteBuffer dst) throws IOException
		{
			final int toPut = Math.min(this.limit - this.position, dst.remaining());
			dst.put(this.buffer, this.position, toPut);
			this.position += toPut;
			return toPut;
		}
		
	}
}
