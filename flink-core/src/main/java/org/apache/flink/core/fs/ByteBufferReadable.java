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
 *
 */

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Experimental;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An interface mark that a filesystem supports to read bytes from a {@link ByteBuffer} and its
 * given position. The interface is borrowed from {@code ByteBufferReadable} and {@code
 * ByteBufferPositionedReadable} in Apache Hadoop.
 */
@Experimental
public interface ByteBufferReadable {

    /**
     * Reads up to byteBuffer.remaining() bytes into byteBuffer. Callers should use
     * byteBuffer.limit(..) to control the size of the desired read.
     *
     * <p>After a successful call, byteBuffer.position() will be advanced by the number of bytes
     * read and byteBuffer.limit() should be unchanged.
     *
     * <p>In the case of an exception, the values of byteBuffer.position() and byteBuffer.limit()
     * are undefined, and callers should be prepared to recover from this eventuality.
     * Implementations should treat 0-length requests as legitimate, and must not signal an error
     * upon their receipt.
     *
     * @param byteBuffer the ByteBuffer to receive the results of the read operation.
     * @return the number of bytes read, possibly zero, or -1 if reach end-of-stream
     * @throws IOException if there is some error performing the read
     */
    int read(ByteBuffer byteBuffer) throws IOException;

    /**
     * Reads up to {@code byteBuffer.remaining()} bytes into byteBuffer from a given position in the
     * file and returns the number of bytes read. Callers should use {@code byteBuffer.limit(...)}
     * to control the size of the desired read and {@code byteBuffer.position(...)} to control the
     * offset into the buffer the data should be written to.
     *
     * <p>After a successful call, {@code byteBuffer.position()} will be advanced by the number of
     * bytes read and {@code byteBuffer.limit()} will be unchanged.
     *
     * <p>In the case of an exception, the state of the buffer (the contents of the buffer, the
     * {@code buf.position()}, the {@code buf.limit()}, etc.) is undefined, and callers should be
     * prepared to recover from this eventuality.
     *
     * <p>Implementations should treat 0-length requests as legitimate, and must not signal an error
     * upon their receipt.
     *
     * @param position position within file
     * @param byteBuffer the ByteBuffer to receive the results of the read operation.
     * @return the number of bytes read, possibly zero, or -1 if reached end-of-stream
     * @throws IOException if there is some error performing the read
     */
    int read(long position, ByteBuffer byteBuffer) throws IOException;
}
