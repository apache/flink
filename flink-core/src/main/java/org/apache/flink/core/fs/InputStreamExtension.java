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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;

import java.io.IOException;

/**
 * Extension point for {@link ObjectStorageInputStream}.
 *
 * <p>All methods are called with the stream's internal lock held.
 *
 * @see ObjectStorageInputStream
 */
@Internal
@Experimental
public interface InputStreamExtension {

    /** Read-only view of stream state passed to extension callbacks. */
    @Internal
    @Experimental
    interface StreamContext {

        /** Returns the current byte position. */
        long getPos();

        /** Returns the total content length in bytes. */
        long getContentLength();
    }

    /**
     * Opens streams at the current {@linkplain StreamContext#getPos() read position}.
     *
     * <p>Called during lazy initialization (first read) and stream recovery (seek beyond
     * threshold). The previous streams are closed by the base class before this call. The base
     * class manages the lifecycle of the returned streams (reads, closes, reopens on seek).
     *
     * <p>If the underlying source cannot do range reads (e.g., encrypted streams opened at offset
     * 0), open at offset 0 and skip forward to {@link StreamContext#getPos()} after wrapping.
     *
     * <p>On failure, close any streams opened during this call before rethrowing.
     *
     * @param ctx read-only view of the stream state at the time of opening
     * @return the raw and wrapped stream pair
     * @throws IOException if opening fails
     */
    RawAndWrappedInputStreams openStream(StreamContext ctx) throws IOException;

    /**
     * Returns the default extension that opens the raw stream via {@code opener} and wraps it with
     * a {@link java.io.BufferedInputStream}.
     *
     * @param opener opens a raw stream at a given byte position
     * @param readBufferSize the buffer size for the {@link java.io.BufferedInputStream}
     */
    static InputStreamExtension buffering(
            final InputStreamOpener opener, final int readBufferSize) {
        return new BufferingInputStreamExtension(opener, readBufferSize);
    }
}
