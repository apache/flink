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

package org.apache.flink.fs.cse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TestingKeyProvider}. */
class TestingKeyProviderTest {

    private static final String TEST_KEY_ID = "test-key-1";
    private static final int NUM_VERSIONS = TestingKeyProvider.PREDEFINED_VERSIONS.size();

    static IntStream versionIndices() {
        return IntStream.range(0, NUM_VERSIONS);
    }

    @Test
    void shouldReturnDifferentKeyAfterRotation() {
        final TestingKeyProvider provider = new TestingKeyProvider();
        final KeyMaterial before = provider.getActiveKey(TEST_KEY_ID, Collections.emptyMap());

        provider.rotateKey();

        final KeyMaterial after = provider.getActiveKey(TEST_KEY_ID, Collections.emptyMap());
        assertThat(after.getVersion()).isNotEqualTo(before.getVersion());
        assertThat(after.getMaterial()).isNotEqualTo(before.getMaterial());
    }

    @ParameterizedTest
    @MethodSource("versionIndices")
    void getKeyForMetadataShouldResolveByVersion(final int versionIndex) throws Exception {
        final TestingKeyProvider provider = new TestingKeyProvider();
        final String version = TestingKeyProvider.PREDEFINED_VERSIONS.get(versionIndex);

        for (int i = 0; i < versionIndex; i++) {
            provider.rotateKey();
        }
        final KeyMaterial fromActive = provider.getActiveKey(TEST_KEY_ID, Collections.emptyMap());
        final KeyMaterial fromMetadata =
                provider.getKeyForMetadata(new TestingEncryptionMetadata(TEST_KEY_ID, version));

        assertThat(fromMetadata.getKeyId()).isEqualTo(TEST_KEY_ID);
        assertThat(fromMetadata.getVersion()).isEqualTo(version);
        assertThat(fromMetadata.getMaterial()).isNotEmpty();
        assertThat(fromMetadata.getMaterial()).isEqualTo(fromActive.getMaterial());
    }

    @Test
    void getKeyForMetadataShouldThrowForUnknownVersion() {
        final TestingKeyProvider provider = new TestingKeyProvider();

        assertThatThrownBy(
                        () ->
                                provider.getKeyForMetadata(
                                        new TestingEncryptionMetadata(
                                                TEST_KEY_ID, "unknown-version")))
                .isInstanceOf(KeyProviderException.class)
                .hasMessageContaining("unknown-version");
    }

    // ---------------------------------------------------------------------------
    // Stub
    // ---------------------------------------------------------------------------

    private static final class TestingEncryptionMetadata implements EncryptionMetadata {

        private final String keyId;
        private final String keyVersion;

        TestingEncryptionMetadata(final String keyId, final String keyVersion) {
            this.keyId = keyId;
            this.keyVersion = keyVersion;
        }

        @Override
        public String keyId() {
            return keyId;
        }

        @Override
        public String getKeyVersion() {
            return keyVersion;
        }

        @Override
        public Map<String, String> getEncryptionContext() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, String> toMetadata() {
            return Collections.emptyMap();
        }
    }
}
