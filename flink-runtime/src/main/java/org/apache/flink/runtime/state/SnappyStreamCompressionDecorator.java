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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.util.NonClosingInputStreamDecorator;
import org.apache.flink.runtime.util.NonClosingOutpusStreamDecorator;

import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** This implementation decorates the stream with snappy compression. */
@Internal
public class SnappyStreamCompressionDecorator extends StreamCompressionDecorator {

    public static final StreamCompressionDecorator INSTANCE =
            new SnappyStreamCompressionDecorator();

    private static final long serialVersionUID = 1L;

    private static final int COMPRESSION_BLOCK_SIZE = 64 * 1024;
    private static final double MIN_COMPRESSION_RATIO = 0.85d;

    @Override
    protected OutputStream decorateWithCompression(NonClosingOutpusStreamDecorator stream)
            throws IOException {
        return new SnappyFramedOutputStream(stream, COMPRESSION_BLOCK_SIZE, MIN_COMPRESSION_RATIO);
    }

    @Override
    protected InputStream decorateWithCompression(NonClosingInputStreamDecorator stream)
            throws IOException {
        return new SnappyFramedInputStream(stream, false);
    }
}
