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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.Immutable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Default {@link InputStreamExtension} that opens the raw stream via an {@link InputStreamOpener}
 * and wraps it with a {@link BufferedInputStream}.
 */
@Internal
@Immutable
final class BufferingInputStreamExtension implements InputStreamExtension {

    private final InputStreamOpener opener;
    private final int readBufferSize;

    BufferingInputStreamExtension(final InputStreamOpener opener, final int readBufferSize) {
        this.opener = Preconditions.checkNotNull(opener, "opener");
        Preconditions.checkArgument(readBufferSize > 0, "readBufferSize must be positive");
        this.readBufferSize = readBufferSize;
    }

    @Override
    public RawAndWrappedInputStreams openStream(final InputStreamExtension.StreamContext ctx)
            throws IOException {
        final InputStream raw = opener.open(ReadContext.of(ctx.getPos()));
        return new RawAndWrappedInputStreams(raw, new BufferedInputStream(raw, readBufferSize));
    }
}
