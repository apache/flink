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
import org.apache.flink.util.function.FunctionWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AesGcmCseInputStreamExtension}. */
class AesGcmCseInputStreamExtensionTest {

    private static final int BUFFER_SIZE = 8192;
    private static final byte[] PLAINTEXT = {1, 2, 3, 4, 5};

    /** {@link InputStream} that tracks {@link #close()} calls. */
    static class CloseTrackingInputStream extends ByteArrayInputStream {
        boolean closed;

        CloseTrackingInputStream(final byte[] buf) {
            super(buf);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    /** {@link InputStream} wrapper that throws on {@link #skipNBytes(long)}. */
    static class FailOnSkipInputStream extends FilterInputStream {
        boolean closed;

        FailOnSkipInputStream(final InputStream in) {
            super(in);
        }

        @Override
        public void skipNBytes(final long n) throws IOException {
            throw new IOException("simulated skip failure");
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    private static InputStreamExtension.StreamContext contextAt(
            final long pos, final long contentLength) {
        return new InputStreamExtension.StreamContext() {
            @Override
            public long getPos() {
                return pos;
            }

            @Override
            public long getContentLength() {
                return contentLength;
            }
        };
    }

    /** Identity decryptor factory: returns the input stream unchanged. */
    private static FunctionWithException<InputStream, InputStream, IOException> identity() {
        return in -> in;
    }

    // --- shouldReadFromPosition ---

    static Stream<Arguments> readPositionCases() {
        return Stream.of(
                Arguments.of(0L, 1, "position zero"), Arguments.of(3L, 4, "skip to middle"));
    }

    @ParameterizedTest
    @MethodSource("readPositionCases")
    void shouldReadFromPosition(final long pos, final int expectedFirstByte, final String label)
            throws IOException {
        final AtomicLong capturedOpenerPos = new AtomicLong(-1);
        final InputStreamOpener opener =
                ctx -> {
                    capturedOpenerPos.set(ctx.getPos());
                    return new ByteArrayInputStream(PLAINTEXT);
                };

        final AesGcmCseInputStreamExtension extension =
                new AesGcmCseInputStreamExtension(opener, BUFFER_SIZE, identity());
        final RawAndWrappedInputStreams streams =
                extension.openStream(contextAt(pos, PLAINTEXT.length));

        // opener always receives offset 0 — CSE streams cannot do range reads
        assertThat(capturedOpenerPos.get()).as(label + ": opener pos").isEqualTo(0L);

        final int firstByte = streams.wrapped().read();
        assertThat(firstByte).as(label + ": first byte").isEqualTo(expectedFirstByte);
    }

    // --- shouldCloseAndPropagateOnDecryptorFailure ---

    @Test
    void shouldCloseAndPropagateOnDecryptorFailure() {
        final IOException closeFailure = new IOException("close failed");
        final CloseTrackingInputStream raw =
                new CloseTrackingInputStream(PLAINTEXT) {
                    @Override
                    public void close() throws IOException {
                        closed = true;
                        throw closeFailure;
                    }
                };
        final IOException decryptorFailure = new IOException("decryptor failed");
        final AesGcmCseInputStreamExtension extension =
                new AesGcmCseInputStreamExtension(
                        ctx -> raw,
                        BUFFER_SIZE,
                        in -> {
                            throw decryptorFailure;
                        });

        assertThatThrownBy(() -> extension.openStream(contextAt(0L, PLAINTEXT.length)))
                .isSameAs(decryptorFailure)
                .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(closeFailure));
        assertThat(raw.closed).isTrue();
    }

    // --- shouldCloseAndPropagateOnSkipFailure ---

    @Test
    void shouldCloseAndPropagateOnSkipFailure() {
        final IOException closeFailure = new IOException("close failed");
        final FailOnSkipInputStream decrypted =
                new FailOnSkipInputStream(new ByteArrayInputStream(PLAINTEXT)) {
                    @Override
                    public void close() throws IOException {
                        closed = true;
                        throw closeFailure;
                    }
                };
        final AesGcmCseInputStreamExtension extension =
                new AesGcmCseInputStreamExtension(
                        ctx -> new ByteArrayInputStream(PLAINTEXT), BUFFER_SIZE, in -> decrypted);

        assertThatThrownBy(() -> extension.openStream(contextAt(100L, 200L)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("simulated skip failure")
                .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(closeFailure));
        assertThat(decrypted.closed).isTrue();
    }

    // --- shouldRejectInvalidConstructorArgs ---

    static Stream<Arguments> invalidConstructorArgs() {
        final InputStreamOpener validOpener = ctx -> new ByteArrayInputStream(new byte[0]);
        return Stream.of(
                Arguments.of(
                        null, BUFFER_SIZE, identity(), NullPointerException.class, "null opener"),
                Arguments.of(
                        validOpener,
                        0,
                        identity(),
                        IllegalArgumentException.class,
                        "non-positive buffer size"),
                Arguments.of(
                        validOpener,
                        BUFFER_SIZE,
                        null,
                        NullPointerException.class,
                        "null decryptorFactory"));
    }

    @ParameterizedTest
    @MethodSource("invalidConstructorArgs")
    void shouldRejectInvalidConstructorArgs(
            final InputStreamOpener opener,
            final int bufferSize,
            final FunctionWithException<InputStream, InputStream, IOException> decryptorFactory,
            final Class<? extends Exception> expectedType,
            final String label) {
        assertThatThrownBy(
                        () ->
                                new AesGcmCseInputStreamExtension(
                                        opener, bufferSize, decryptorFactory))
                .as(label)
                .isInstanceOf(expectedType);
    }
}
