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
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** This class contains unit tests for the {@link BlobKey} class. */
final class BlobKeyTest {
    /** The first key array to be used during the unit tests. */
    private static final byte[] KEY_ARRAY_1 = new byte[BlobKey.SIZE];

    /** The second key array to be used during the unit tests. */
    private static final byte[] KEY_ARRAY_2 = new byte[BlobKey.SIZE];

    /** First byte array to use for the random component of a {@link BlobKey}. */
    private static final byte[] RANDOM_ARRAY_1 = new byte[AbstractID.SIZE];

    /** Second byte array to use for the random component of a {@link BlobKey}. */
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
    void testCreateKey() {
        BlobKey key = BlobKey.createKey(PERMANENT_BLOB, KEY_ARRAY_1);
        verifyType(PERMANENT_BLOB, key);
        assertThat(key.getHash()).isEqualTo(KEY_ARRAY_1);

        key = BlobKey.createKey(TRANSIENT_BLOB, KEY_ARRAY_1);
        verifyType(TRANSIENT_BLOB, key);
        assertThat(key.getHash()).isEqualTo(KEY_ARRAY_1);
    }

    @Test
    void testSerializationTransient() throws Exception {
        testSerialization(TRANSIENT_BLOB);
    }

    @Test
    void testSerializationPermanent() throws Exception {
        testSerialization(PERMANENT_BLOB);
    }

    /** Tests the serialization/deserialization of BLOB keys. */
    private void testSerialization(BlobKey.BlobType blobType) throws Exception {
        final BlobKey k1 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_1);
        final BlobKey k2 = CommonTestUtils.createCopySerializable(k1);
        assertThat(k2).isEqualTo(k1);
        assertThat(k2).hasSameHashCodeAs(k1);
        assertThat(k1).isEqualByComparingTo(k2);
    }

    @Test
    void testEqualsTransient() {
        testEquals(TRANSIENT_BLOB);
    }

    @Test
    void testEqualsPermanent() {
        testEquals(PERMANENT_BLOB);
    }

    /** Tests the {@link BlobKey#equals(Object)} and {@link BlobKey#hashCode()} methods. */
    private void testEquals(BlobKey.BlobType blobType) {
        final BlobKey k1 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_1);
        final BlobKey k2 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_1);
        final BlobKey k3 = BlobKey.createKey(blobType, KEY_ARRAY_2, RANDOM_ARRAY_1);
        final BlobKey k4 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_2);
        assertThat(k1).isEqualTo(k2);
        assertThat(k2).isEqualTo(k1);
        assertThat(k2).hasSameHashCodeAs(k1);
        assertThat(k1).isNotEqualTo(k3);
        assertThat(k3).isNotEqualTo(k1);
        assertThat(k1).isNotEqualTo(k4);
        assertThat(k4).isNotEqualTo(k1);

        assertThat(k1).isNotEqualTo(null);
        //noinspection AssertBetweenInconvertibleTypes
        assertThat(k1).isNotEqualTo(this);
    }

    /** Tests the equals method. */
    @Test
    void testEqualsDifferentBlobType() {
        final BlobKey k1 = BlobKey.createKey(TRANSIENT_BLOB, KEY_ARRAY_1, RANDOM_ARRAY_1);
        final BlobKey k2 = BlobKey.createKey(PERMANENT_BLOB, KEY_ARRAY_1, RANDOM_ARRAY_1);
        assertThat(k1).isNotEqualTo(k2);
        assertThat(k2).isNotEqualTo(k1);
    }

    @Test
    void testComparesTransient() {
        testCompares(TRANSIENT_BLOB);
    }

    @Test
    void testComparesPermanent() {
        testCompares(PERMANENT_BLOB);
    }

    /** Tests the compares method. */
    private void testCompares(BlobKey.BlobType blobType) {
        final BlobKey k1 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_1);
        final BlobKey k2 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_1);
        final BlobKey k3 = BlobKey.createKey(blobType, KEY_ARRAY_2, RANDOM_ARRAY_1);
        final BlobKey k4 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_2);
        assertThat(k1).isEqualByComparingTo(k2);
        assertThat(k2).isEqualByComparingTo(k1);
        assertThat(k1).isLessThan(k3);
        assertThat(k1).isLessThan(k4);
        assertThat(k3).isGreaterThan(k1);
        assertThat(k4).isGreaterThan(k1);
    }

    @Test
    void testComparesDifferentBlobType() {
        final BlobKey k1 = BlobKey.createKey(TRANSIENT_BLOB, KEY_ARRAY_1, RANDOM_ARRAY_1);
        final BlobKey k2 = BlobKey.createKey(PERMANENT_BLOB, KEY_ARRAY_1, RANDOM_ARRAY_1);
        assertThat(k1).isGreaterThan(k2);
        assertThat(k2).isLessThan(k1);
    }

    @Test
    void testStreamsTransient() throws Exception {
        testStreams(TRANSIENT_BLOB);
    }

    @Test
    void testStreamsPermanent() throws Exception {
        testStreams(PERMANENT_BLOB);
    }

    @Test
    void testToFromStringPermanentKey() {
        testToFromString(BlobKey.createKey(PERMANENT_BLOB));
    }

    @Test
    void testToFromStringTransientKey() {
        testToFromString(BlobKey.createKey(TRANSIENT_BLOB));
    }

    private void testToFromString(BlobKey blobKey) {
        final String stringRepresentation = blobKey.toString();
        final BlobKey parsedBlobKey = BlobKey.fromString(stringRepresentation);

        assertThat(blobKey).isEqualTo(parsedBlobKey);
    }

    @Test
    void testFromStringFailsWithWrongInput() {
        assertThatThrownBy(() -> BlobKey.fromString("foobar"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testFromStringFailsWithInvalidBlobKeyType() {
        assertThatThrownBy(
                        () ->
                                BlobKey.fromString(
                                        String.format(
                                                "x-%s-%s",
                                                StringUtils.byteToHexString(KEY_ARRAY_1),
                                                StringUtils.byteToHexString(RANDOM_ARRAY_1))))
                .isInstanceOf(IllegalStateException.class);
    }

    /** Test the serialization/deserialization using input/output streams. */
    private void testStreams(BlobKey.BlobType blobType) throws IOException {
        final BlobKey k1 = BlobKey.createKey(blobType, KEY_ARRAY_1, RANDOM_ARRAY_1);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(20);

        k1.writeToOutputStream(baos);
        baos.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final BlobKey k2 = BlobKey.readFromInputStream(bais);

        assertThat(k2).isEqualTo(k1);
    }

    /**
     * Verifies that the two given key's are different in total but share the same hash.
     *
     * @param key1 first blob key
     * @param key2 second blob key
     */
    static void verifyKeyDifferentHashEquals(BlobKey key1, BlobKey key2) {
        assertThat(key1).isNotEqualTo(key2);
        assertThat(key1.getHash()).isEqualTo(key2.getHash());
    }

    /**
     * Verifies that the two given key's are different in total and also have different hashes.
     *
     * @param key1 first blob key
     * @param key2 second blob key
     */
    static void verifyKeyDifferentHashDifferent(BlobKey key1, BlobKey key2) {
        assertThat(key1).isNotEqualTo(key2);
        assertThat(key1.getHash()).isNotEqualTo(key2.getHash());
    }

    /**
     * Verifies that the given <tt>key</tt> is of an expected type.
     *
     * @param expected the type the key should have
     * @param key the key to verify
     */
    static void verifyType(BlobKey.BlobType expected, BlobKey key) {
        if (expected == PERMANENT_BLOB) {
            assertThat(key).isInstanceOf(PermanentBlobKey.class);
        } else {
            assertThat(key).isInstanceOf(TransientBlobKey.class);
        }
    }
}
