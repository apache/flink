/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.DuplicatingCheckpointOutputStream;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.BiFunction;

/**
 * A DuplicatingOutputStreamWithPos is similar to {@link DuplicatingCheckpointOutputStream} which
 * wraps a primary and a secondary OutputStream and duplicates all writes into both streams. The
 * difference is that this stream does not delete the file when {@link #close()}.
 */
class DuplicatingOutputStreamWithPos extends OutputStreamWithPos {
    private static final Logger LOG = LoggerFactory.getLogger(DuplicatingOutputStreamWithPos.class);

    private OutputStream secondaryStream;
    private OutputStream originalSecondaryStream;
    private final Path secondaryPath;

    /**
     * Stores a potential exception that occurred while interacting with {@link #secondaryStream}.
     */
    private Exception secondaryStreamException;

    public DuplicatingOutputStreamWithPos(
            OutputStream primaryStream,
            Path primaryPath,
            OutputStream secondaryStream,
            Path secondaryPath) {
        super(primaryStream, primaryPath);
        this.secondaryStream = Preconditions.checkNotNull(secondaryStream);
        this.originalSecondaryStream = Preconditions.checkNotNull(secondaryStream);
        this.secondaryPath = Preconditions.checkNotNull(secondaryPath);
    }

    @Override
    public void wrap(boolean compression, int bufferSize) throws IOException {
        super.wrap(compression, bufferSize);
        this.secondaryStream = wrapInternal(compression, bufferSize, this.originalSecondaryStream);
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
        if (secondaryStreamException == null) {
            try {
                secondaryStream.write(b);
            } catch (Exception ex) {
                handleSecondaryStreamOnException(ex);
            }
        }
        pos++;
    }

    @Override
    public void write(byte[] b) throws IOException {
        outputStream.write(b);
        if (secondaryStreamException == null) {
            try {
                secondaryStream.write(b);
            } catch (Exception ex) {
                LOG.warn("Exception encountered during write to secondary stream");
                handleSecondaryStreamOnException(ex);
            }
        }
        pos += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        outputStream.write(b, off, len);
        if (secondaryStreamException == null) {
            try {
                secondaryStream.write(b, off, len);
            } catch (Exception ex) {
                LOG.warn("Exception encountered during writing to secondary stream");
                handleSecondaryStreamOnException(ex);
            }
        }
        pos += len;
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
        if (secondaryStreamException == null) {
            try {
                secondaryStream.flush();
            } catch (Exception ex) {
                LOG.warn("Exception encountered during flushing secondary stream");
                handleSecondaryStreamOnException(ex);
            }
        }
    }

    @Override
    public void close() throws IOException {
        Exception exCollector = null;

        try {
            super.close();
        } catch (Exception closeEx) {
            exCollector = ExceptionUtils.firstOrSuppressed(closeEx, exCollector);
        }

        if (secondaryStreamException == null) {
            try {
                secondaryStream.close();
                originalSecondaryStream.close();
            } catch (Exception closeEx) {
                getSecondaryPath().getFileSystem().delete(getSecondaryPath(), true);
                handleSecondaryStreamOnException(closeEx);
            }
        }

        if (exCollector != null) {
            throw new IOException("Exception while closing duplicating stream.", exCollector);
        }
    }

    private void handleSecondaryStreamOnException(Exception ex) {

        Preconditions.checkState(
                secondaryStreamException == null,
                "Secondary stream already failed from previous exception!");
        try {
            secondaryStream.close();
        } catch (Exception closeEx) {
            ex = ExceptionUtils.firstOrSuppressed(closeEx, ex);
        }

        secondaryStreamException = Preconditions.checkNotNull(ex);
    }

    public Path getSecondaryPath() {
        return secondaryPath;
    }

    public StreamStateHandle getSecondaryHandle(
            BiFunction<Path, Long, StreamStateHandle> handleFactory) throws IOException {
        if (secondaryStreamException == null) {
            return handleFactory.apply(secondaryPath, this.pos);
        } else {
            throw new IOException(
                    "Secondary stream previously failed exceptionally", secondaryStreamException);
        }
    }
}
