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
import org.apache.flink.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An {@link OutputStream} that keeps track of its current length. */
@Internal
public final class OffsetAwareOutputStream implements Closeable {

    private final OutputStream currentOut;

    private long position;

    OffsetAwareOutputStream(OutputStream currentOut, long position) {
        this.currentOut = checkNotNull(currentOut);
        this.position = position;
    }

    public long getLength() {
        return position;
    }

    public void write(byte[] b, int off, int len) throws IOException {
        currentOut.write(b, off, len);
        position += len;
    }

    public void flush() throws IOException {
        currentOut.flush();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(currentOut);
    }
}
