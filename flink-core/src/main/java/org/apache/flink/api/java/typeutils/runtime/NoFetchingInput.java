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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoBufferUnderflowException;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

@Internal
public class NoFetchingInput extends Input {
    public NoFetchingInput(InputStream inputStream) {
        super(inputStream, 8);
    }

    @Override
    public int read() throws KryoException {
        require(1);
        return buffer[position++] & 0xFF;
    }

    @Override
    public boolean canReadInt() throws KryoException {
        throw new UnsupportedOperationException("NoFetchingInput cannot prefetch data.");
    }

    @Override
    public boolean canReadLong() throws KryoException {
        throw new UnsupportedOperationException("NoFetchingInput cannot prefetch data.");
    }

    /**
     * Require makes sure that at least required number of bytes are kept in the buffer. If not,
     * then it will load exactly the difference between required and currently available number of
     * bytes. Thus, it will only load the data which is required and never prefetch data.
     *
     * @param required the number of bytes being available in the buffer
     * @return The number of bytes remaining in the buffer, which will be at least <code>required
     *     </code> bytes.
     * @throws KryoException
     */
    @Override
    protected int require(int required) throws KryoException {
        // The main change between this and Kryo 5 Input.require is this will never read more bytes
        // than required.
        // There are also formatting changes to be compliant with the Flink project styling rules.
        int remaining = limit - position;
        if (remaining >= required) {
            return remaining;
        }
        if (required > capacity) {
            throw new KryoException(
                    "Buffer too small: capacity: " + capacity + ", required: " + required);
        }

        int count;
        // Try to fill the buffer.
        if (remaining > 0) {
            // Logical change 1 (from Kryo Input.require): "capacity - limit" -> "required - limit"
            count = fill(buffer, limit, required - limit);
            if (count == -1) {
                throw new KryoBufferUnderflowException("Buffer underflow.");
            }
            remaining += count;
            if (remaining >= required) {
                limit += count;
                return remaining;
            }
        }

        // Was not enough, compact and try again.
        System.arraycopy(buffer, position, buffer, 0, remaining);
        total += position;
        position = 0;

        do {
            // Logical change 2 (from Kryo Input.require): "capacity - remaining" -> "required -
            // remaining"
            count = fill(buffer, remaining, required - remaining);
            if (count == -1) {
                throw new KryoBufferUnderflowException("Buffer underflow.");
            }
            remaining += count;
        } while (remaining < required);

        limit = remaining;
        return remaining;
    }

    @Override
    public int read(byte[] bytes, int offset, int count) throws KryoException {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes cannot be null.");
        }

        try {
            return inputStream.read(bytes, offset, count);
        } catch (IOException ex) {
            throw new KryoException(ex);
        }
    }

    @Override
    public void skip(int count) throws KryoException {
        try {
            inputStream.skip(count);
        } catch (IOException ex) {
            throw new KryoException(ex);
        }
    }

    @Override
    public void readBytes(byte[] bytes, int offset, int count) throws KryoException {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes cannot be null.");
        }

        if (count == 0) {
            return;
        }

        try {
            int bytesRead = 0;
            int c;

            while (true) {
                c = inputStream.read(bytes, offset + bytesRead, count - bytesRead);

                if (c == -1) {
                    throw new KryoException(new EOFException("No more bytes left."));
                }

                bytesRead += c;

                if (bytesRead == count) {
                    break;
                }
            }
        } catch (IOException ex) {
            throw new KryoException(ex);
        }
    }
}
