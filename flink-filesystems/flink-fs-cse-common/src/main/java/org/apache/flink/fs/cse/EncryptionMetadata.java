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

import java.util.Map;

/**
 * Per-file encryption metadata stored in cloud object metadata.
 *
 * <p>Implementations define how the data encryption key is referenced (e.g., by key ID + version
 * for key-vending providers, or as a wrapped DEK blob for envelope encryption providers) and which
 * cipher algorithm is used.
 *
 * <p>Serialization to/from a flat {@code Map<String, String>} is the common wire format, supported
 * by all major cloud storage APIs (Azure blob metadata, S3 user metadata, GCS custom metadata).
 */
@Internal
@Experimental
public interface EncryptionMetadata {

    /** Returns the encryption key identifier. */
    String keyId();

    /**
     * Returns the key version identifier used to retrieve the correct decryption key.
     *
     * <p>Implementations that do not use key versioning (e.g., envelope encryption where the DEK is
     * wrapped in the metadata itself) may return an empty string.
     */
    String getKeyVersion();

    /** Returns an unmodifiable view of the encryption context, empty if none was set. */
    Map<String, String> getEncryptionContext();

    /** Converts this metadata to a map suitable for cloud object metadata. */
    Map<String, String> toMetadata();
}
