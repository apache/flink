/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.util.Preconditions;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.BiFunction;

class OutputStreamWithPos extends OutputStream {
    protected final Path path;
    protected OutputStream outputStream;
    protected long pos;
    protected boolean compression;
    protected final OutputStream originalStream;

    public OutputStreamWithPos(OutputStream outputStream, Path path) {
        this.outputStream = Preconditions.checkNotNull(outputStream);
        this.originalStream = Preconditions.checkNotNull(outputStream);
        this.path = Preconditions.checkNotNull(path);
        this.pos = 0;
        this.compression = false;
    }

    protected OutputStream wrapInternal(boolean compression, int bufferSize, OutputStream fsStream)
            throws IOException {
        fsStream.write(compression ? 1 : 0);
        StreamCompressionDecorator instance =
                compression
                        ? SnappyStreamCompressionDecorator.INSTANCE
                        : UncompressedStreamCompressionDecorator.INSTANCE;
        return new BufferedOutputStream(instance.decorateWithCompression(fsStream), bufferSize);
    }

    public void wrap(boolean compression, int bufferSize) throws IOException {
        this.compression = compression;
        this.outputStream = wrapInternal(compression, bufferSize, this.originalStream);
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
        pos++;
    }

    @Override
    public void write(byte[] b) throws IOException {
        outputStream.write(b);
        pos += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        outputStream.write(b, off, len);
        pos += len;
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            outputStream.close();
            originalStream.close();
        } catch (IOException e) {
            getPath().getFileSystem().delete(getPath(), true);
        }
    }

    public long getPos() {
        return pos;
    }

    public Path getPath() {
        return path;
    }

    public StreamStateHandle getHandle(BiFunction<Path, Long, StreamStateHandle> handleFactory) {
        return handleFactory.apply(path, this.pos);
    }
}
