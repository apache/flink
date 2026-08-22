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

import javax.annotation.concurrent.Immutable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Carries all context required to open an encrypted blob for reading.
 *
 * @see CseStreamFactory#openEncryptedRead(org.apache.flink.core.fs.InputStreamOpener,
 *     EncryptedReadContext)
 */
@Internal
@Experimental
@Immutable
public interface EncryptedReadContext {

    /** Returns the blob's metadata map containing encryption headers. */
    Map<String, String> getBlobMetadata();

    /** Returns the cloud object path (used for logging and error messages). */
    String getFilePath();

    /** Returns the total ciphertext length in bytes. */
    long getCiphertextLength();

    /** Returns the read buffer size in bytes. */
    int getReadBufferSize();

    /**
     * Creates an {@link EncryptedReadContext} with the given values.
     *
     * @param blobMetadata the blob's metadata map containing encryption headers
     * @param filePath the cloud object path
     * @param ciphertextLength the total ciphertext length in bytes; must be &ge; 0
     * @param readBufferSize the read buffer size in bytes; must be &gt; 0
     * @return a new immutable {@link EncryptedReadContext}
     */
    static EncryptedReadContext of(
            final Map<String, String> blobMetadata,
            final String filePath,
            final long ciphertextLength,
            final int readBufferSize) {
        checkNotNull(blobMetadata, "blobMetadata");
        checkNotNull(filePath, "filePath");
        checkArgument(ciphertextLength >= 0, "ciphertextLength must be >= 0");
        checkArgument(readBufferSize > 0, "readBufferSize must be > 0");
        final Map<String, String> unmodifiable =
                Collections.unmodifiableMap(new HashMap<>(blobMetadata));
        return new EncryptedReadContext() {
            @Override
            public Map<String, String> getBlobMetadata() {
                return unmodifiable;
            }

            @Override
            public String getFilePath() {
                return filePath;
            }

            @Override
            public long getCiphertextLength() {
                return ciphertextLength;
            }

            @Override
            public int getReadBufferSize() {
                return readBufferSize;
            }
        };
    }
}
