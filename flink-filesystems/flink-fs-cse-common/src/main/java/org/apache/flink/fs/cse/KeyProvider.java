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

import java.io.Closeable;
import java.util.Map;

/**
 * Provider of encryption keys used in client-side encryption of filesystem data.
 *
 * <p>This interface abstracts the key management backend. Implementations may delegate to a KMS
 * (e.g., AWS KMS, Azure Key Vault, GCP Cloud KMS, or a custom key vending service). For testing, a
 * deterministic key implementation is provided.
 *
 * <p>Implementations must be thread-safe, as the same provider instance is shared across the
 * filesystem.
 *
 * @see KeyProviderFactory
 */
@Internal
@Experimental
@ThreadSafe
public interface KeyProvider extends Closeable {

    /**
     * Returns the current active key for encrypting new files (write path).
     *
     * @param keyId the key identifier (e.g., master key reference or key ARN)
     * @param encryptionContext context for KMS authorization; empty map if none
     * @throws KeyProviderException if the key cannot be retrieved
     */
    KeyMaterial getActiveKey(String keyId, Map<String, String> encryptionContext)
            throws KeyProviderException;

    /**
     * Returns the plaintext key referenced by per-file encryption metadata (read path).
     *
     * @throws KeyProviderException if the key cannot be retrieved
     */
    KeyMaterial getKeyForMetadata(EncryptionMetadata metadata) throws KeyProviderException;
}
