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
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.InputStreamOpener;
import org.apache.flink.core.fs.OutputStreamOpener;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * Algorithm-agnostic factory for client-side encrypted streams.
 *
 * <p>Write path: call {@link #openEncryptedWrite(OutputStreamOpener, EncryptedWriteContext)} to
 * obtain an encrypting {@link OutputStream}. The factory generates key material, builds a {@link
 * org.apache.flink.core.fs.WriteContext} containing the encryption metadata, invokes the opener
 * with that context so the caller can attach metadata to the cloud object, and wraps the returned
 * stream with encryption.
 *
 * <p>Read path: call {@link #isEncrypted(Map)} to detect whether a blob is encrypted, then call
 * {@link #openEncryptedRead} to obtain a decrypting {@link FSDataInputStream}.
 *
 * <p>Implementations must be thread-safe — a single instance is shared across the filesystem.
 * Closing the factory releases the underlying {@link KeyProvider}.
 */
@Internal
@Experimental
@ThreadSafe
public interface CseStreamFactory extends Closeable {

    /**
     * Opens an encrypted output stream for writing.
     *
     * <p>The factory generates key material, invokes {@code opener} with a {@link
     * org.apache.flink.core.fs.WriteContext} carrying the encryption metadata, and wraps the
     * returned stream with encryption. The returned stream must be closed by the caller to finalize
     * the ciphertext (e.g., to append the GCM authentication tag).
     *
     * @param opener opens the underlying cloud output stream; receives encryption metadata via
     *     {@link org.apache.flink.core.fs.WriteContext#getMetadata()}
     * @param ctx write context carrying the file path and other write-time metadata
     * @return an encrypting {@link OutputStream} wrapping the stream returned by {@code opener}
     * @throws IOException if key material cannot be obtained or the stream cannot be initialized
     */
    OutputStream openEncryptedWrite(OutputStreamOpener opener, EncryptedWriteContext ctx)
            throws IOException;

    /**
     * Returns {@code true} if the blob described by {@code blobMetadata} was encrypted by a
     * compatible factory.
     *
     * @param blobMetadata the cloud object's metadata map
     * @return {@code true} if the blob is encrypted; {@code false} otherwise
     */
    boolean isEncrypted(Map<String, String> blobMetadata);

    /**
     * Opens an encrypted blob for reading and returns a seekable decrypting stream.
     *
     * <p>The caller must verify {@link #isEncrypted(Map)} returns {@code true} before calling this
     * method.
     *
     * @param opener opens a raw ciphertext stream at a given byte offset
     * @param ctx read context carrying blob metadata, file path, ciphertext length, and buffer size
     * @return a seekable, decrypting {@link FSDataInputStream} over the plaintext
     * @throws IOException if key material cannot be obtained or the stream cannot be initialized
     */
    FSDataInputStream openEncryptedRead(InputStreamOpener opener, EncryptedReadContext ctx)
            throws IOException;
}
