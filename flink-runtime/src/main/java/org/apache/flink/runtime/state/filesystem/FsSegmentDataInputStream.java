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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataInputStreamWrapper;
import org.apache.flink.core.fs.WrappingProxyCloseable;

import java.io.IOException;

/**
 * This class is a {@link org.apache.flink.util.WrappingProxy} for {@link FSDataInputStream} that is
 * used to read from a file segment. It is opened with a starting position of the file. It treats
 * the argument of seek(long) as an offset relative to the starting position, rather than an
 * absolute value.
 */
public class FsSegmentDataInputStream extends FSDataInputStreamWrapper
        implements WrappingProxyCloseable<FSDataInputStream> {

    private final long startingPosition;

    private long endingPosition;

    public FsSegmentDataInputStream(
            FSDataInputStream inputStream, long startingPosition, long segmentSize)
            throws IOException {
        super(inputStream);
        if (startingPosition < 0 || segmentSize < 0) {
            throw new IndexOutOfBoundsException(
                    "Invalid startingPosition/segmentSize: "
                            + startingPosition
                            + "/"
                            + segmentSize);
        }
        this.startingPosition = startingPosition;
        this.endingPosition = startingPosition + segmentSize;
        inputStream.seek(startingPosition);
    }

    @Override
    public int read() throws IOException {
        if (inputStream.getPos() >= endingPosition) {
            return -1;
        }
        int result = inputStream.read();
        if (result == -1) {
            return -1;
        } else {
            return result;
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        return this.read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }

        int n =
                (int)
                        Math.min(
                                Integer.MAX_VALUE,
                                Math.min(len, (endingPosition - inputStream.getPos())));
        if (n == 0) {
            return -1;
        }
        int ret = inputStream.read(b, off, n);
        if (ret < 0) {
            endingPosition = inputStream.getPos();
            return -1;
        }
        return ret;
    }

    @Override
    public void seek(long desired) throws IOException {
        desired += startingPosition;
        inputStream.seek(desired);
    }

    @Override
    public long getPos() throws IOException {
        return inputStream.getPos() - startingPosition;
    }

    @Override
    public long skip(long n) throws IOException {
        long len = Math.min(n, endingPosition - inputStream.getPos());
        return inputStream.skip(n);
    }

    @Override
    public synchronized void mark(int readlimit) {
        inputStream.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        inputStream.reset();
    }

    @Override
    public boolean markSupported() {
        return inputStream.markSupported();
    }
}
