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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class contains unit tests for the {@link BlobKey} class.
 */
public final class BlobKeyTest extends TestLogger {
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
	 * Initialize the key and random arrays.
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

	@Test
	public void testCreateKey() {
		BlobKey key = BlobKey.createKey(PERMANENT_BLOB, KEY_ARRAY_1);
		verifyType(PERMANENT_BLOB, key);
		assertArrayEquals(KEY_ARRAY_1, key.getHash());

		key = BlobKey.createKey(TRANSIENT_BLOB, KEY_ARRAY_1);
		verifyType(TRANSIENT_BLOB, key);
		assertArrayEquals(KEY_ARRAY_1, key.getHash());

	}

	@Test
	public void testSerializationTransient() throws Exception {
		testSerialization(TRANSIENT_BLOB);
	}

	@Test
	public void testSerializationPermanent() throws Exception {
		testSerialization(PERMANENT_BLOB);
	}

	/**
	 * Tests the serialization/deserialization of BLOB keys.
	 */
	private void testSerialization(BlobKey.BlobType blobType) throws Exception {
		final BlobKey k1 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_1);
		final BlobKey k2 = CommonTestUtils.createCopySerializable(k1);
		assertEquals(k1, k2);
		assertEquals(k1.hashCode(), k2.hashCode());
		assertEquals(0, k1.compareTo(k2));
	}

	@Test
	public void testEqualsTransient() {
		testEquals(TRANSIENT_BLOB);
	}

	@Test
	public void testEqualsPermanent() {
		testEquals(PERMANENT_BLOB);
	}

	/**
	 * Tests the {@link BlobKey#equals(Object)} and {@link BlobKey#hashCode()} methods.
	 */
	private void testEquals(BlobKey.BlobType blobType) {
		final BlobKey k1 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_1);
		final BlobKey k2 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_1);
		final BlobKey k3 = BlobKey.createKey(blobType, KEY_ARRAY_2, RANDOM_ARRAY_1);
		final BlobKey k4 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_2);
		assertTrue(k1.equals(k2));
		assertTrue(k2.equals(k1));
		assertEquals(k1.hashCode(), k2.hashCode());
		assertFalse(k1.equals(k3));
		assertFalse(k3.equals(k1));
		assertFalse(k1.equals(k4));
		assertFalse(k4.equals(k1));

		//noinspection ObjectEqualsNull
		assertFalse(k1.equals(null));
		//noinspection EqualsBetweenInconvertibleTypes
		assertFalse(k1.equals(this));
	}

	/**
	 * Tests the equals method.
	 */
	@Test
	public void testEqualsDifferentBlobType() {
		final BlobKey k1 = BlobKey.createKey(TRANSIENT_BLOB, KEY_ARRAY_1, RANDOM_ARRAY_1);
		final BlobKey k2 = BlobKey.createKey(PERMANENT_BLOB, KEY_ARRAY_1, RANDOM_ARRAY_1);
		assertFalse(k1.equals(k2));
		assertFalse(k2.equals(k1));
	}

	@Test
	public void testComparesTransient() {
		testCompares(TRANSIENT_BLOB);
	}

	@Test
	public void testComparesPermanent() {
		testCompares(PERMANENT_BLOB);
	}

	/**
	 * Tests the compares method.
	 */
	private void testCompares(BlobKey.BlobType blobType) {
		final BlobKey k1 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_1);
		final BlobKey k2 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_1);
		final BlobKey k3 = BlobKey.createKey(blobType, KEY_ARRAY_2, RANDOM_ARRAY_1);
		final BlobKey k4 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_2);
		assertThat(k1.compareTo(k2), is(0));
		assertThat(k2.compareTo(k1), is(0));
		assertThat(k1.compareTo(k3), lessThan(0));
		assertThat(k1.compareTo(k4), lessThan(0));
		assertThat(k3.compareTo(k1), greaterThan(0));
		assertThat(k4.compareTo(k1), greaterThan(0));
	}

	@Test
	public void testComparesDifferentBlobType() {
		final BlobKey k1 = BlobKey.createKey(TRANSIENT_BLOB, KEY_ARRAY_1, RANDOM_ARRAY_1);
		final BlobKey k2 = BlobKey.createKey(PERMANENT_BLOB, KEY_ARRAY_1, RANDOM_ARRAY_1);
		assertThat(k1.compareTo(k2), greaterThan(0));
		assertThat(k2.compareTo(k1), lessThan(0));
	}

	@Test
	public void testStreamsTransient() throws Exception {
		testStreams(TRANSIENT_BLOB);
	}

	@Test
	public void testStreamsPermanent() throws Exception {
		testStreams(PERMANENT_BLOB);
	}

	/**
	 * Test the serialization/deserialization using input/output streams.
	 */
	private void testStreams(BlobKey.BlobType blobType) throws IOException {
		final BlobKey k1 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_1);
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
		assertThat(key1.getHash(), equalTo(key2.getHash()));
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

	/**
	 * Verifies that the given <tt>key</tt> is of an expected type.
	 *
	 * @param expected the type the key should have
	 * @param key      the key to verify
	 */
	static void verifyType(BlobKey.BlobType expected, BlobKey key) {
		if (expected == PERMANENT_BLOB) {
			assertThat(key, is(instanceOf(PermanentBlobKey.class)));
		} else {
			assertThat(key, is(instanceOf(TransientBlobKey.class)));
		}
	}
}
