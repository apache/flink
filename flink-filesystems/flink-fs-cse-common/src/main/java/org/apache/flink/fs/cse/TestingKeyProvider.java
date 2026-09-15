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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A deterministic {@link KeyProvider} for testing CSE encryption with key rotation.
 *
 * <p>Uses predefined ULID versions that rotate in round-robin fashion. Keys are derived
 * deterministically from their version string (UTF-8 bytes, padded to 32 bytes), so the same
 * version always produces the same 256-bit key. This allows reading encrypted files after
 * application restart and testing multi-version file access.
 *
 * <p><b>WARNING: This provider is for TESTING ONLY.</b> Keys are derived from predictable version
 * strings with no real key management.
 *
 * <p>This class is placed in main sources (not test sources) so it is available on the plugin
 * classpath for reflective instantiation from Flink configuration in E2E and manual tests.
 */
@Experimental
@Internal
@ThreadSafe
public final class TestingKeyProvider implements KeyProvider {

    /** Predefined ULID versions for deterministic rotation (Crockford Base32). */
    static final List<String> PREDEFINED_VERSIONS =
            List.of(
                    "01ARZ3NDEKTSV4RRFFQ69G5FA1",
                    "01ARZ3NDEKTSV4RRFFQ69G5FA2",
                    "01ARZ3NDEKTSV4RRFFQ69G5FA3");

    /** AES-256 requires 32 bytes. */
    private static final int KEY_LENGTH_BYTES = 32;

    private final Map<String, byte[]> keyMaterials;
    private final AtomicInteger currentVersionIndex;

    /** Creates a testing key provider with all predefined versions pre-derived. */
    public TestingKeyProvider() {
        this.currentVersionIndex = new AtomicInteger(0);

        final Map<String, byte[]> materials = new HashMap<>();
        for (final String version : PREDEFINED_VERSIONS) {
            materials.put(version, deriveKeyFromVersion(version));
        }
        this.keyMaterials = Collections.unmodifiableMap(materials);
    }

    @Override
    public KeyMaterial getActiveKey(
            final String keyId, final Map<String, String> encryptionContext) {
        final String version = PREDEFINED_VERSIONS.get(currentVersionIndex.get());
        return new KeyMaterial(keyId, version, keyMaterials.get(version));
    }

    @Override
    public KeyMaterial getKeyForMetadata(final EncryptionMetadata metadata)
            throws KeyProviderException {
        final String storedKeyId = metadata.keyId();
        final String keyVersion = metadata.getKeyVersion();
        final byte[] material = keyMaterials.get(keyVersion);
        if (material == null) {
            throw new KeyProviderException(
                    "Unknown key version '" + keyVersion + "' for key '" + storedKeyId + "'");
        }
        return new KeyMaterial(storedKeyId, keyVersion, material);
    }

    @Override
    public void close() {
        // No resources to release.
    }

    /**
     * Rotates to the next predefined version in round-robin fashion.
     *
     * @return the version string of the new current version
     */
    public String rotateKey() {
        final int nextIndex =
                currentVersionIndex.updateAndGet(i -> (i + 1) % PREDEFINED_VERSIONS.size());
        return PREDEFINED_VERSIONS.get(nextIndex);
    }

    /**
     * Returns the current version index (0-based).
     *
     * @return index of the current version in {@link #PREDEFINED_VERSIONS}
     */
    public int getCurrentVersionIndex() {
        return currentVersionIndex.get();
    }

    private static byte[] deriveKeyFromVersion(final String version) {
        final byte[] utf8 = version.getBytes(StandardCharsets.UTF_8);
        final byte[] key = Arrays.copyOf(utf8, KEY_LENGTH_BYTES);
        for (int i = utf8.length; i < KEY_LENGTH_BYTES; i++) {
            key[i] = (byte) i;
        }
        return key;
    }
}
