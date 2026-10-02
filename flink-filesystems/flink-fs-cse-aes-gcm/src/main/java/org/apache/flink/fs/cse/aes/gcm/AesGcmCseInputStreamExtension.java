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

package org.apache.flink.fs.cse.aes.gcm;

import org.apache.flink.core.fs.InputStreamExtension;
import org.apache.flink.core.fs.InputStreamOpener;
import org.apache.flink.core.fs.RawAndWrappedInputStreams;
import org.apache.flink.core.fs.ReadContext;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * {@link InputStreamExtension} that opens a raw stream at offset 0, wraps it with buffering, and
 * applies CSE decryption. On seek, the decrypting stream is reopened from the start and skipped
 * forward.
 */
@NotThreadSafe
final class AesGcmCseInputStreamExtension implements InputStreamExtension {

    private final InputStreamOpener opener;
    private final int readBufferSize;
    private final FunctionWithException<InputStream, InputStream, IOException> decryptorFactory;

    AesGcmCseInputStreamExtension(
            final InputStreamOpener opener,
            final int readBufferSize,
            final FunctionWithException<InputStream, InputStream, IOException> decryptorFactory) {
        this.opener = Preconditions.checkNotNull(opener, "opener");
        Preconditions.checkArgument(readBufferSize > 0, "readBufferSize must be positive");
        this.readBufferSize = readBufferSize;
        this.decryptorFactory = Preconditions.checkNotNull(decryptorFactory, "decryptorFactory");
    }

    @Override
    public RawAndWrappedInputStreams openStream(final StreamContext ctx) throws IOException {
        final InputStream raw = opener.open(ReadContext.of(0L));
        final InputStream buffered = new BufferedInputStream(raw, readBufferSize);
        final InputStream decrypting = applyOrCloseOnFailure(buffered, decryptorFactory);

        if (ctx.getPos() > 0L) {
            skipToPositionOrCloseOnFailure(decrypting, ctx.getPos());
        }

        return new RawAndWrappedInputStreams(raw, decrypting);
    }

    /**
     * Applies {@code factory} to {@code resource}, closing {@code resource} and adding the close
     * exception as suppressed if the factory throws.
     */
    private static InputStream applyOrCloseOnFailure(
            final InputStream resource,
            final FunctionWithException<InputStream, InputStream, IOException> factory)
            throws IOException {
        try {
            return factory.apply(resource);
        } catch (final IOException | RuntimeException e) {
            closeAndAddSuppressedOnFailure(resource, e);
            throw e;
        }
    }

    /**
     * Skips {@code n} bytes on {@code resource}, closing it and adding the close exception as
     * suppressed if skip throws.
     */
    private static void skipToPositionOrCloseOnFailure(final InputStream resource, final long n)
            throws IOException {
        try {
            resource.skipNBytes(n);
        } catch (final IOException | RuntimeException e) {
            closeAndAddSuppressedOnFailure(resource, e);
            throw e;
        }
    }

    private static void closeAndAddSuppressedOnFailure(
            final Closeable resource, final Throwable primary) {
        try {
            resource.close();
        } catch (final Exception suppressed) {
            primary.addSuppressed(suppressed);
        }
    }
}
