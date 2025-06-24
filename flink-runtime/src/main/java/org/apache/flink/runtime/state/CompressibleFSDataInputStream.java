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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link FSDataInputStream} that delegates all reading operations to a wrapping {@link
 * StreamCompressionDecorator}.
 */
public class CompressibleFSDataInputStream extends FSDataInputStream {

    private final FSDataInputStream delegate;
    private final InputStream compressingDelegate;
    private final boolean compressed;

    public CompressibleFSDataInputStream(
            FSDataInputStream delegate, StreamCompressionDecorator compressionDecorator)
            throws IOException {
        this.delegate = delegate;
        this.compressingDelegate = compressionDecorator.decorateWithCompression(delegate);
        this.compressed = compressionDecorator != UncompressedStreamCompressionDecorator.INSTANCE;
    }

    @Override
    public void seek(long desired) throws IOException {
        if (compressed) {
            final int available = compressingDelegate.available();
            if (available > 0 && available != compressingDelegate.skip(available)) {
                throw new IOException("Unable to skip buffered data.");
            }
        }
        delegate.seek(desired);
    }

    @Override
    public long getPos() throws IOException {
        return delegate.getPos();
    }

    @Override
    public int read() throws IOException {
        return compressingDelegate.read();
    }

    @Override
    public void close() throws IOException {
        compressingDelegate.close();
    }
}
