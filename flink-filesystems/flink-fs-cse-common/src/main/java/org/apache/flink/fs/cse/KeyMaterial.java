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
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

/**
 * Holds key material returned by a {@link KeyProvider}: the key identifier, version, and plaintext
 * key bytes.
 *
 * <p>The key ID and version are stored in file metadata so the correct key can be retrieved on the
 * read path. The key material is the raw symmetric key (e.g., 32 bytes for AES-256).
 */
@Internal
@Experimental
@Immutable
public final class KeyMaterial {

    private final String keyId;
    private final String version;
    private final byte[] material;

    /**
     * @param material the plaintext key bytes (e.g., 32 bytes for AES-256); defensively copied
     */
    public KeyMaterial(final String keyId, final String version, final byte[] material) {
        this.keyId = Preconditions.checkNotNull(keyId, "keyId must not be null");
        this.version = Preconditions.checkNotNull(version, "version must not be null");
        Preconditions.checkNotNull(material, "material must not be null");
        Preconditions.checkArgument(material.length > 0, "material must not be empty");
        this.material = material.clone();
    }

    /** Returns the key identifier. */
    public String getKeyId() {
        return keyId;
    }

    /** Returns the key version identifier. */
    public String getVersion() {
        return version;
    }

    /** Returns a copy of the plaintext key bytes. */
    public byte[] getMaterial() {
        return material.clone();
    }

    /**
     * Equality is based on key identity (keyId + version) only — material is excluded to avoid
     * timing side-channels on secret bytes.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KeyMaterial)) {
            return false;
        }
        final KeyMaterial that = (KeyMaterial) o;
        return Objects.equals(keyId, that.keyId) && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyId, version);
    }

    /** Returns a redacted string — key bytes are never included. */
    @Override
    public String toString() {
        return "KeyMaterial{keyId=" + keyId + ", version=" + version + "}";
    }
}
