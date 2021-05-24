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

package org.apache.flink.formats.parquet;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FSDataOutputStream;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of Parquet's {@link OutputFile} interface that goes against a Flink {@link
 * FSDataOutputStream}.
 *
 * <p>Because the implementation goes against an open stream, rather than open its own streams
 * against a file, instances can create one stream only.
 */
@Internal
class StreamOutputFile implements OutputFile {

    private static final long DEFAULT_BLOCK_SIZE = 64L * 1024L * 1024L;

    private final FSDataOutputStream stream;

    private final AtomicBoolean used;

    /**
     * Creates a new StreamOutputFile. The first call to {@link #create(long)} or {@link
     * #createOrOverwrite(long)} returns a stream that writes to the given stream.
     *
     * @param stream The stream to write to.
     */
    StreamOutputFile(FSDataOutputStream stream) {
        this.stream = checkNotNull(stream);
        this.used = new AtomicBoolean(false);
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
        if (used.compareAndSet(false, true)) {
            return new PositionOutputStreamAdapter(stream);
        } else {
            throw new IllegalStateException("A stream against this file was already created.");
        }
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return DEFAULT_BLOCK_SIZE;
    }
}
