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

package org.apache.flink.runtime.blob;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.AbstractID;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * This class contains unit tests for the {@link BlobKey} class.
 */
public final class BlobKeyTest {
	/**
	 * The first key array to be used during the unit tests.
	 */
	private static final byte[] KEY_ARRAY_1 = new byte[BlobKey.SIZE];

	/**
	 * The second key array to be used during the unit tests.
	 */
	private static final byte[] KEY_ARRAY_2 = new byte[BlobKey.SIZE];

	/**
	 * First byte array to use for the random component of a {@link BlobKey}.
	 */
	private static final byte[] RANDOM_ARRAY_1 = new byte[AbstractID.SIZE];

	/**
	 * Second byte array to use for the random component of a {@link BlobKey}.
	 */
	private static final byte[] RANDOM_ARRAY_2 = new byte[AbstractID.SIZE];

	/*
	  Initialize the key and random arrays.
	 */
	static {
		for (int i = 0; i < KEY_ARRAY_1.length; ++i) {
			KEY_ARRAY_1[i] = (byte) i;
			KEY_ARRAY_2[i] = (byte) (i + 1);
		}
		for (int i = 0; i < RANDOM_ARRAY_1.length; ++i) {
			RANDOM_ARRAY_1[i] = (byte) i;
			RANDOM_ARRAY_2[i] = (byte) (i + 1);
		}
	}

	/**
	 * Tests the serialization/deserialization of BLOB keys
	 */
	@Test
	public void testSerialization() throws Exception {
		final BlobKey k1 = new BlobKey(KEY_ARRAY_1, RANDOM_ARRAY_1);
		final BlobKey k2 = CommonTestUtils.createCopySerializable(k1);
		assertEquals(k1, k2);
		assertEquals(k1.hashCode(), k2.hashCode());
		assertEquals(0, k1.compareTo(k2));
	}

	/**
	 * Tests the {@link BlobKey#equals(Object)} and {@link BlobKey#hashCode()} methods.
	 */
	@Test
	public void testEquals() {
		final BlobKey k1 = new BlobKey(KEY_ARRAY_1, RANDOM_ARRAY_1);
		final BlobKey k2 = new BlobKey(KEY_ARRAY_1, RANDOM_ARRAY_1);
		final BlobKey k3 = new BlobKey(KEY_ARRAY_2, RANDOM_ARRAY_1);
		final BlobKey k4 = new BlobKey(KEY_ARRAY_1, RANDOM_ARRAY_2);
		assertTrue(k1.equals(k2));
		assertEquals(k1.hashCode(), k2.hashCode());
		assertFalse(k1.equals(k3));
		assertFalse(k1.equals(k4));

		//noinspection ObjectEqualsNull
		assertFalse(k1.equals(null));
		//noinspection EqualsBetweenInconvertibleTypes
		assertFalse(k1.equals(this));
	}

	/**
	 * Tests the compares method.
	 */
	@Test
	public void testCompares() {
		final BlobKey k1 = new BlobKey(KEY_ARRAY_1, RANDOM_ARRAY_1);
		final BlobKey k2 = new BlobKey(KEY_ARRAY_1, RANDOM_ARRAY_1);
		final BlobKey k3 = new BlobKey(KEY_ARRAY_2, RANDOM_ARRAY_1);
		final BlobKey k4 = new BlobKey(KEY_ARRAY_1, RANDOM_ARRAY_2);
		assertTrue(k1.compareTo(k2) == 0);
		assertTrue(k1.compareTo(k3) < 0);
		assertTrue(k1.compareTo(k4) < 0);
		assertTrue(k3.compareTo(k1) > 0);
		assertTrue(k4.compareTo(k1) > 0);
	}

	/**
	 * Test the serialization/deserialization using input/output streams.
	 */
	@Test
	public void testStreams() throws Exception {
		final BlobKey k1 = new BlobKey(KEY_ARRAY_1, RANDOM_ARRAY_1);
		final ByteArrayOutputStream baos = new ByteArrayOutputStream(20);
		
		k1.writeToOutputStream(baos);
		baos.close();
		
		final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		final BlobKey k2 = BlobKey.readFromInputStream(bais);

		assertEquals(k1, k2);
	}

	/**
	 * Verifies that the two given key's are different in total but share the same hash.
	 *
	 * @param key1 first blob key
	 * @param key2 second blob key
	 */
	static void verifyKeyDifferentHashEquals(BlobKey key1, BlobKey key2) {
		assertNotEquals(key1, key2);
		assertArrayEquals(key1.getHash(), key2.getHash());
	}

	/**
	 * Verifies that the two given key's are different in total and also have different hashes.
	 *
	 * @param key1 first blob key
	 * @param key2 second blob key
	 */
	static void verifyKeyDifferentHashDifferent(BlobKey key1, BlobKey key2) {
		assertNotEquals(key1, key2);
		assertThat(key1.getHash(), not(equalTo(key2.getHash())));
	}
}
