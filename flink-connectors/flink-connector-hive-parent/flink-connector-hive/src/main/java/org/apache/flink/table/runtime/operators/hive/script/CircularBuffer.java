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

package org.apache.flink.table.runtime.operators.hive.script;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/** A circular buffer with limit size to store the last written bytes (default is 10 kb). */
public class CircularBuffer extends OutputStream {
    private static final int DEFAULT_SIZE_IN_BYTES = 10240;

    private final int sizeInBytes;
    private boolean bufferEverFull = false;
    private final byte[] buffer;
    private int pos = 0;

    public CircularBuffer() {
        this(DEFAULT_SIZE_IN_BYTES);
    }

    public CircularBuffer(int sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
        this.buffer = new byte[sizeInBytes];
    }

    @Override
    public void write(int b) throws IOException {
        buffer[pos] = (byte) b;
        pos = (pos + 1) % buffer.length;
        bufferEverFull = bufferEverFull || pos == 0;
    }

    @Override
    public String toString() {
        // if the buffer isn't ever full, read the buffer directly
        if (!bufferEverFull) {
            return new String(buffer, 0, pos, StandardCharsets.UTF_8);
        }
        // else, split the buffer into two segments,
        // and first read right segment, and then read left segment
        byte[] nonCircularBuffer = new byte[sizeInBytes];
        System.arraycopy(buffer, pos, nonCircularBuffer, 0, buffer.length - pos);
        System.arraycopy(buffer, 0, nonCircularBuffer, buffer.length - pos, pos);
        return new String(nonCircularBuffer, StandardCharsets.UTF_8);
    }
}
