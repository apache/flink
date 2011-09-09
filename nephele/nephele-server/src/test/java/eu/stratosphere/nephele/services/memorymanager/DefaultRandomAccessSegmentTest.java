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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;

public class DefaultRandomAccessSegmentTest {
	
	public static final long RANDOM_SEED = 643196033469871L;

	public static final int MANAGED_MEMORY_SIZE = 1024 * 1024 * 16;

	public static final int SEGMENT_SIZE = 1024 * 512;

	private DefaultMemoryManager manager;

	private MemorySegment segment;

	private Random random;

	@Before
	public void setUp() {
		try {
			manager = new DefaultMemoryManager(MANAGED_MEMORY_SIZE);
			segment = manager.allocate(new DefaultMemoryManagerTest.DummyInvokable(), SEGMENT_SIZE);
			random = new Random(RANDOM_SEED);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@After
	public void tearDown() {
		manager.release(segment);

		random = null;
		segment = null;
		manager = null;
	}

	@Test
	public void bulkByteAccess() {

		// test exceptions
		{
			byte[] bytes = new byte[SEGMENT_SIZE / 4];

			try {
				segment.randomAccessView.put(3 * (SEGMENT_SIZE / 4) + 1, bytes);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.put(7 * (SEGMENT_SIZE / 8) + 1, bytes, 0, bytes.length / 2);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior with default offset / length
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			byte[] src = new byte[SEGMENT_SIZE / 8];
			for (int i = 0; i < 8; i++) {
				random.nextBytes(src);
				segment.randomAccessView.put(i * (SEGMENT_SIZE / 8), src);
			}

			random.setSeed(seed);
			byte[] expected = new byte[SEGMENT_SIZE / 8];
			byte[] actual = new byte[SEGMENT_SIZE / 8];
			for (int i = 0; i < 8; i++) {
				random.nextBytes(expected);
				segment.randomAccessView.get(i * (SEGMENT_SIZE / 8), actual);

				assertArrayEquals(expected, actual);
			}
		}

		// test expected correct behavior with specific offset / length
		{
			byte[] expected = new byte[SEGMENT_SIZE];
			random.nextBytes(expected);

			for (int i = 0; i < 16; i++) {
				segment.randomAccessView.put(i * (SEGMENT_SIZE / 16), expected, i * (SEGMENT_SIZE / 16),
					SEGMENT_SIZE / 16);
			}

			byte[] actual = new byte[SEGMENT_SIZE];
			for (int i = 0; i < 16; i++) {
				segment.randomAccessView.get(i * (SEGMENT_SIZE / 16), actual, i * (SEGMENT_SIZE / 16),
					SEGMENT_SIZE / 16);
			}

			assertArrayEquals(expected, actual);
		}
	}

	@Test
	public void byteAccess() {
		// test exceptions
		{
			try {
				segment.randomAccessView.put(-1, (byte) 0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.put(SEGMENT_SIZE, (byte) 0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.get(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.get(SEGMENT_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE; i++) {
				segment.randomAccessView.put(i, (byte) random.nextInt());
			}

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE; i++) {
				assertEquals((byte) random.nextInt(), segment.randomAccessView.get(i));
			}
		}
	}

	@Test
	public void booleanAccess() {
		// test exceptions
		{
			try {
				segment.randomAccessView.putBoolean(-1, false);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.putBoolean(SEGMENT_SIZE, false);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getBoolean(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getBoolean(SEGMENT_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE; i++) {
				segment.randomAccessView.putBoolean(i, random.nextBoolean());
			}

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE; i++) {
				assertEquals(random.nextBoolean(), segment.randomAccessView.getBoolean(i));
			}
		}
	}

	@Test
	public void charAccess() {
		// test exceptions
		{
			try {
				segment.randomAccessView.putChar(-1, 'a');
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.putChar(SEGMENT_SIZE, 'a');
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getChar(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getChar(SEGMENT_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE / 2; i += 2) {
				segment.randomAccessView.putChar(i, (char) ('a' + random.nextInt(26)));
			}

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE / 2; i += 2) {
				assertEquals((char) ('a' + random.nextInt(26)), segment.randomAccessView.getChar(i));
			}
		}
	}

	@Test
	public void doubleAccess() {
		// test exceptions
		{
			try {
				segment.randomAccessView.putDouble(-1, 0.0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.putDouble(SEGMENT_SIZE, 0.0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getDouble(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getDouble(SEGMENT_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE / 8; i += 8) {
				segment.randomAccessView.putDouble(i, random.nextDouble());
			}

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE / 8; i += 8) {
				assertEquals(random.nextDouble(), segment.randomAccessView.getDouble(i), 0.0);
			}
		}
	}

	// @Test
	public void floatAccess() {
		// test exceptions
		{
			try {
				segment.randomAccessView.putFloat(-1, 0.0f);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.putFloat(SEGMENT_SIZE, 0.0f);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getFloat(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getFloat(SEGMENT_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE / 4; i += 4) {
				segment.randomAccessView.putFloat(i, random.nextFloat());
			}

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE / 4; i += 4) {
				assertEquals(random.nextFloat(), segment.randomAccessView.getFloat(i), 0.0);
			}
		}
	}

	@Test
	public void longAccess() {
		// test exceptions
		{
			try {
				segment.randomAccessView.putLong(-1, 0L);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.putLong(SEGMENT_SIZE, 0L);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getLong(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getLong(SEGMENT_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE / 8; i += 8) {
				segment.randomAccessView.putLong(i, random.nextLong());
			}

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE / 8; i += 8) {
				assertEquals(random.nextLong(), segment.randomAccessView.getLong(i));
			}
		}
	}

	@Test
	public void intAccess() {
		// test exceptions
		{
			try {
				segment.randomAccessView.putInt(-1, 0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.putInt(SEGMENT_SIZE, 0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getInt(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getInt(SEGMENT_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE / 4; i += 4) {
				segment.randomAccessView.putInt(i, random.nextInt());
			}

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE / 4; i += 4) {
				assertEquals(random.nextInt(), segment.randomAccessView.getInt(i));
			}
		}
	}

	@Test
	public void shortAccess() {
		// test exceptions
		{
			try {
				segment.randomAccessView.putShort(-1, (short) 0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.putShort(SEGMENT_SIZE, (short) 0);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getShort(-1);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}

			try {
				segment.randomAccessView.getShort(SEGMENT_SIZE);
				fail("IndexOutOfBoundsException expected");
			} catch (Exception e) {
				assertTrue(e instanceof IndexOutOfBoundsException);
			}
		}

		// test expected correct behavior
		{
			long seed = random.nextLong();

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE / 4; i += 4) {
				segment.randomAccessView.putShort(i, (short) random.nextInt());
			}

			random.setSeed(seed);
			for (int i = 0; i < SEGMENT_SIZE / 4; i += 4) {
				assertEquals((short) random.nextInt(), segment.randomAccessView.getShort(i));
			}
		}
	}
}
