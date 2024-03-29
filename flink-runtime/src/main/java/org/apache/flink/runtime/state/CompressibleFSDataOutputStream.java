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

import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link FSDataOutputStream} that delegates all writing operations to a wrapping {@link
 * StreamCompressionDecorator}.
 */
public class CompressibleFSDataOutputStream extends FSDataOutputStream {

    private final FSDataOutputStream delegate;
    private final OutputStream compressingDelegate;

    public CompressibleFSDataOutputStream(
            FSDataOutputStream delegate, StreamCompressionDecorator compressionDecorator)
            throws IOException {
        this.delegate = delegate;
        this.compressingDelegate = compressionDecorator.decorateWithCompression(delegate);
    }

    @Override
    public long getPos() throws IOException {
        // Underlying compression involves buffering, so the only way to report correct position is
        // to flush the underlying stream. This lowers the effectivity of compression, but there is
        // no other way, since the position is often used as a split point.
        flush();
        return delegate.getPos();
    }

    @Override
    public void write(int b) throws IOException {
        compressingDelegate.write(b);
    }

    @Override
    public void flush() throws IOException {
        compressingDelegate.flush();
    }

    @Override
    public void sync() throws IOException {
        delegate.sync();
    }

    @Override
    public void close() throws IOException {
        compressingDelegate.close();
    }
}
