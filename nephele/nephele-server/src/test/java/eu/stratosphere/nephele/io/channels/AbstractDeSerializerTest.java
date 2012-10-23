/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.io.channels;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import org.junit.Before;

import eu.stratosphere.nephele.io.channels.serialization.BooleanType;
import eu.stratosphere.nephele.io.channels.serialization.ByteArrayType;
import eu.stratosphere.nephele.io.channels.serialization.ByteSubArrayType;
import eu.stratosphere.nephele.io.channels.serialization.ByteType;
import eu.stratosphere.nephele.io.channels.serialization.CharType;
import eu.stratosphere.nephele.io.channels.serialization.DoubleType;
import eu.stratosphere.nephele.io.channels.serialization.FloatType;
import eu.stratosphere.nephele.io.channels.serialization.IntType;
import eu.stratosphere.nephele.io.channels.serialization.LongType;
import eu.stratosphere.nephele.io.channels.serialization.SerializationTestType;
import eu.stratosphere.nephele.io.channels.serialization.ShortType;
import eu.stratosphere.nephele.io.channels.serialization.UTFStringType;
import eu.stratosphere.nephele.io.channels.serialization.UnsignedByteType;
import eu.stratosphere.nephele.io.channels.serialization.UnsignedShortType;

public abstract class AbstractDeSerializerTest {

	protected static final SerializationTestType[] TYPE_FACTORIES = new SerializationTestType[] {
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

	protected static final class IntTypeIterator implements Iterator<SerializationTestType> {

		private final Random random;

		private final int limit;

		private int pos = 0;

		protected IntTypeIterator(final Random random, final int limit) {
			this.random = random;
			this.limit = limit;
		}

		@Override
		public boolean hasNext() {
			return this.pos < this.limit;
		}

		@Override
		public IntType next() {
			if (hasNext()) {
				++this.pos;
				return new IntType(this.random.nextInt());
			}

			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	protected static final class RandomTypeIterator implements Iterator<SerializationTestType> {

		private final Random random;

		private final int limit;

		private int pos = 0;

		protected RandomTypeIterator(final Random random, final int limit) {
			this.random = random;
			this.limit = limit;
		}

		@Override
		public boolean hasNext() {
			return this.pos < this.limit;
		}

		@Override
		public SerializationTestType next() {
			if (hasNext()) {
				++this.pos;
				return TYPE_FACTORIES[this.random.nextInt(TYPE_FACTORIES.length)].getRandom(this.random);
			}

			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	protected static final class TestBufferPoolConnector implements MemoryBufferPoolConnector {

		@Override
		public void recycle(final ByteBuffer byteBuffer) {
		}

	}

	/**
	 * The number of integers to write in the tests
	 */
	protected static final int NUM_INTS = 1000000;

	/**
	 * The number of types to write in the tests
	 */
	protected final int NUM_TYPES = 1000000;

	/**
	 * The seed to initialize the random number generator with.
	 */
	private static final long SEED = 64871654635745873L;

	/**
	 * The random number generator to be used during the tests.
	 */
	protected Random rnd;

	// --------------------------------------------------------------------------------------------

	@Before
	public void setup() {

		this.rnd = new Random(SEED);
	}

	protected void testSequenceOfTypes(final Iterator<SerializationTestType> sequence, final int bufferSize,
			final boolean copyToFileBuffer) throws Exception {

		final FileBufferManager fbm = FileBufferManager.getInstance();
		final ChannelID ownerID = ChannelID.generate();
		final RecordSerializer<SerializationTestType> recordSerializer = createSerializer();
		final RecordDeserializer<SerializationTestType> recordDeserializer = createDeserializer();

		final ArrayDeque<SerializationTestType> elements = new ArrayDeque<SerializationTestType>(512);
		final ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
		final TestBufferPoolConnector bufferPoolConnector = new TestBufferPoolConnector();
		Buffer buffer = BufferFactory.createFromMemory(bufferSize, byteBuffer, bufferPoolConnector);

		while (sequence.hasNext()) {
			final SerializationTestType type = sequence.next();

			// serialize the record
			recordSerializer.serialize(type);
			elements.addLast(type);

			// write the serialized record
			while (true) {
				recordSerializer.read(buffer);
				if (recordSerializer.dataLeftFromPreviousSerialization()) {
					// current buffer is full, we need to start de-serializing to make space
					buffer.finishWritePhase();
					if (copyToFileBuffer) {
						final FileBuffer fileBuffer = BufferFactory.createFromFile(bufferSize, ownerID, fbm, false,
							true);
						buffer.copyToBuffer(fileBuffer);
						fileBuffer.finishWritePhase();
						buffer = fileBuffer;
					}

					while (!elements.isEmpty()) {
						final SerializationTestType reference = elements.pollFirst();
						final SerializationTestType result = recordDeserializer.readData(reference.getClass()
							.newInstance(), buffer);
						if (result == null) {
							// not yet complete, we need to break
							elements.addFirst(reference);
							break;
						} else {
							// validate that we deserialized correctly
							assertEquals("The deserialized element is not equal to the serialized element.", reference,
								result);
						}
					}

					byteBuffer.clear();
					buffer = BufferFactory.createFromMemory(bufferSize, byteBuffer, bufferPoolConnector);
				} else {
					break;
				}
			}
		}

		// check the remaining records in the buffers...
		buffer.finishWritePhase();
		if (copyToFileBuffer) {
			final FileBuffer fileBuffer = BufferFactory.createFromFile(bufferSize, ownerID, fbm, false,
				true);
			buffer.copyToBuffer(fileBuffer);
			fileBuffer.finishWritePhase();
			buffer = fileBuffer;
		}
		while (!elements.isEmpty()) {
			final SerializationTestType reference = elements.pollFirst();
			final SerializationTestType result = recordDeserializer
				.readData(reference.getClass().newInstance(), buffer);

			assertNotNull(result);
			assertEquals("The deserialized element is not equal to the serialized element.", reference, result);
		}
	}

	/**
	 * Creates and returns the {@link RecordSerializer} implementation to be used during the tests.
	 * 
	 * @return the record serializer to be used during the tests
	 */
	protected abstract RecordSerializer<SerializationTestType> createSerializer();

	/**
	 * Creates and returns the {@link RecordDeserializer} implementation to be used during the tests.
	 * 
	 * @return the record deserializer to be used during the tests
	 */
	protected abstract RecordDeserializer<SerializationTestType> createDeserializer();
}
