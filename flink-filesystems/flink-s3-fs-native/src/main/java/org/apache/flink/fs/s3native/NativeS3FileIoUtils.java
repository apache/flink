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

package org.apache.flink.fs.s3native;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Shared local-filesystem helpers for the S3 download paths (bulk copy and single-object get).
 *
 * <p>Downloads stream the S3 object body to disk through a bounded heap {@code byte[]} rather than
 * the SDK's {@code AsynchronousFileChannel}-based {@code toFile} transformer, which caches an
 * unbounded per-thread temporary direct buffer sized to each write and can exhaust direct memory
 * during large restores. Keeping this logic in one place ensures both download paths stay bounded.
 */
@Internal
public final class NativeS3FileIoUtils {

    private NativeS3FileIoUtils() {}

    /**
     * Copies all bytes from {@code in} to {@code destination}, overwriting any existing file, using
     * a fixed-size heap buffer. The caller retains ownership of {@code in} and is responsible for
     * closing (or aborting) it — this method only closes the destination stream so the correct
     * close-vs-abort decision can be made based on success or failure.
     */
    public static void copyStream(InputStream in, Path destination, int bufferSize)
            throws IOException {
        try (OutputStream out = Files.newOutputStream(destination)) {
            IOUtils.copyBytes(in, out, bufferSize, false);
        }
    }

    /** Moves {@code source} onto {@code destination}, preferring an atomic move when supported. */
    public static void moveFile(Path source, Path destination) throws IOException {
        try {
            Files.move(
                    source,
                    destination,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Creates a temporary download file in {@code parent} whose name is derived from {@code
     * destination}. The prefix is padded to satisfy {@link Files#createTempFile}'s minimum length.
     */
    public static Path createTemporaryDownloadFile(Path parent, Path destination)
            throws IOException {
        String prefix =
                destination.getFileName() == null
                        ? "s3-download"
                        : destination.getFileName().toString();
        if (prefix.length() < 3) {
            prefix = "s3-" + prefix;
        }
        return Files.createTempFile(parent, prefix, ".tmp");
    }
}
