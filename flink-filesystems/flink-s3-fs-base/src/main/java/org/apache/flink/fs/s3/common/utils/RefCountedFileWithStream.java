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

package org.apache.flink.fs.s3.common.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.RefCountedFile;
import org.apache.flink.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A reference counted file which is deleted as soon as no caller holds a reference to the wrapped
 * {@link File}.
 */
@Internal
public class RefCountedFileWithStream extends RefCountedFile {

    private final OffsetAwareOutputStream stream;

    private RefCountedFileWithStream(
            final File file, final OutputStream currentOut, final long bytesInCurrentPart) {
        super(file);
        this.stream = new OffsetAwareOutputStream(currentOut, bytesInCurrentPart);
    }

    public OffsetAwareOutputStream getStream() {
        return stream;
    }

    public long getLength() {
        return stream.getLength();
    }

    public void write(byte[] b, int off, int len) throws IOException {
        requireOpened();
        if (len > 0) {
            stream.write(b, off, len);
        }
    }

    void flush() throws IOException {
        requireOpened();
        stream.flush();
    }

    void closeStream() {
        if (!closed) {
            IOUtils.closeQuietly(stream);
            closed = true;
        }
    }

    private void requireOpened() throws IOException {
        if (closed) {
            throw new IOException("Stream closed.");
        }
    }

    // ------------------------------ Factory methods for initializing a temporary file
    // ------------------------------

    public static RefCountedFileWithStream newFile(final File file, final OutputStream currentOut)
            throws IOException {
        return new RefCountedFileWithStream(file, currentOut, 0L);
    }

    public static RefCountedFileWithStream restoredFile(
            final File file, final OutputStream currentOut, final long bytesInCurrentPart) {
        return new RefCountedFileWithStream(file, currentOut, bytesInCurrentPart);
    }
}
