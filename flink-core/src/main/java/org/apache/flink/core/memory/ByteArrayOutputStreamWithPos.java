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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.util.Preconditions;

import java.io.OutputStream;
import java.util.Arrays;

/**
 * Un-synchronized stream similar to Java's ByteArrayOutputStream that also exposes the current
 * position.
 */
@Internal
public class ByteArrayOutputStreamWithPos extends OutputStream {

    protected byte[] buffer;
    protected int count;

    public ByteArrayOutputStreamWithPos() {
        this(64);
    }

    public ByteArrayOutputStreamWithPos(int size) {
        Preconditions.checkArgument(size >= 0);
        buffer = new byte[size];
    }

    private void ensureCapacity(int requiredCapacity) {
        if (requiredCapacity - buffer.length > 0) {
            increaseCapacity(requiredCapacity);
        }
    }

    private void increaseCapacity(int requiredCapacity) {
        int oldCapacity = buffer.length;
        int newCapacity = oldCapacity << 1;
        if (newCapacity - requiredCapacity < 0) {
            newCapacity = requiredCapacity;
        }
        if (newCapacity < 0) {
            if (requiredCapacity < 0) {
                throw new OutOfMemoryError();
            }
            newCapacity = Integer.MAX_VALUE;
        }
        buffer = Arrays.copyOf(buffer, newCapacity);
    }

    @Override
    public void write(int b) {
        ensureCapacity(count + 1);
        buffer[count] = (byte) b;
        ++count;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if ((off < 0) || (len < 0) || (off > b.length) || ((off + len) - b.length > 0)) {
            throw new IndexOutOfBoundsException();
        }

        ensureCapacity(count + len);

        System.arraycopy(b, off, buffer, count, len);
        count += len;
    }

    public void reset() {
        count = 0;
    }

    public byte toByteArray()[] {
        return Arrays.copyOf(buffer, count);
    }

    public int size() {
        return count;
    }

    public String toString() {
        return new String(buffer, 0, count, ConfigConstants.DEFAULT_CHARSET);
    }

    public int getPosition() {
        return count;
    }

    public void setPosition(int position) {
        Preconditions.checkArgument(position >= 0, "Position out of bounds.");
        ensureCapacity(position + 1);
        count = position;
    }

    @Override
    public void close() {}

    public byte[] getBuf() {
        return buffer;
    }
}
