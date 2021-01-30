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

package org.apache.flink.fs.gshadoop.writer;

import org.apache.flink.shaded.guava18.com.google.common.io.ByteArrayDataOutput;
import org.apache.flink.shaded.guava18.com.google.common.io.ByteStreams;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

/** A mocked implementation of a file in cloud storage. */
class MockGSBlob {

    private ByteArrayDataOutput data;
    private boolean closed;

    MockGSBlob() {
        data = ByteStreams.newDataOutput();
        closed = false;
    }

    void setPosition(int position) throws IOException {
        if (closed) {
            throw new ClosedChannelException();
        }
        byte[] bytes = data.toByteArray();
        if (bytes.length > position) {

            // reconstruct the data output up to the specified position
            data = ByteStreams.newDataOutput();
            data.write(bytes, 0, position);

        } else if (bytes.length < position) {

            // this is an error state, we can't move forward
            throw new IOException(
                    String.format(
                            "Position %d is invalid in output of length %d",
                            position, bytes.length));
        }
    }

    void write(byte[] bytes, int start, int length) throws IOException {
        if (closed) {
            throw new ClosedChannelException();
        }
        data.write(bytes, start, length);
    }

    void close() throws IOException {
        closed = true;
    }

    byte[] toByteArray() {
        return data.toByteArray();
    }
}
