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

package org.apache.flink.state.forst.fs.cache;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemoryUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link FSDataOutputStream} delegates requests to other one and supports writing data with
 * {@link ByteBuffer}.
 */
public class CachedDataOutputStream extends FSDataOutputStream {

    /** The unsafe handle for transparent memory copied (heap / off-heap). */
    @SuppressWarnings("restriction")
    private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

    /** The beginning of the byte array contents, relative to the byte array object. */
    @SuppressWarnings("restriction")
    private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private static final int SEGMENT_BUFFER_SIZE = 16 * 1024;

    /** The original path of file. */
    private final Path originalPath;

    /** The path in cache. */
    private final Path cachePath;

    private FSDataOutputStream cacheOutputStream;
    private FSDataOutputStream originOutputStream;

    /** The reference of file cache. */
    private FileBasedCache fileBasedCache;

    public CachedDataOutputStream(
            Path originalPath,
            Path cachePath,
            FSDataOutputStream originalOutputStream,
            FSDataOutputStream cacheOutputStream,
            FileBasedCache cache) {
        this.originOutputStream = originalOutputStream;
        this.originalPath = originalPath;
        this.cachePath = cachePath;
        this.cacheOutputStream = cacheOutputStream;
        this.fileBasedCache = cache;
    }

    @Override
    public long getPos() throws IOException {
        return cacheOutputStream.getPos();
    }

    @Override
    public void write(int b) throws IOException {
        cacheOutputStream.write(b);
        originOutputStream.write(b);
    }

    public void write(byte[] b) throws IOException {
        cacheOutputStream.write(b);
        originOutputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        cacheOutputStream.write(b, off, len);
        originOutputStream.write(b, off, len);
    }

    public void write(ByteBuffer bb) throws IOException {
        if (bb.hasArray()) {
            write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
        } else if (bb.isDirect()) {
            int len = bb.remaining();
            int segment = Math.min(len, SEGMENT_BUFFER_SIZE);
            byte[] bytes = new byte[segment];
            for (int i = 0; i < len; ) {
                int copy = Math.min(segment, bb.remaining());
                UNSAFE.copyMemory(
                        null,
                        MemoryUtils.getByteBufferAddress(bb) + bb.position(),
                        bytes,
                        BYTE_ARRAY_BASE_OFFSET,
                        copy);
                write(bytes, 0, copy);
                bb.position(bb.position() + copy);
                i += copy;
            }
        } else {
            byte[] tmp = new byte[bb.remaining()];
            bb.get(tmp);
            write(tmp, 0, tmp.length);
        }
    }

    @Override
    public void flush() throws IOException {
        cacheOutputStream.flush();
        originOutputStream.flush();
    }

    @Override
    public void sync() throws IOException {
        cacheOutputStream.sync();
        originOutputStream.sync();
    }

    @Override
    public void close() throws IOException {
        if (originOutputStream != null) {
            originOutputStream.close();
            originOutputStream = null;
        }
        if (cacheOutputStream != null) {
            putIntoCache();
            cacheOutputStream.close();
            cacheOutputStream = null;
        }
    }

    private void putIntoCache() throws IOException {
        long thisSize = cacheOutputStream.getPos();
        FileCacheEntry fileCacheEntry =
                new FileCacheEntry(fileBasedCache, originalPath, cachePath, thisSize);
        fileBasedCache.put(cachePath.toString(), fileCacheEntry);
    }
}
