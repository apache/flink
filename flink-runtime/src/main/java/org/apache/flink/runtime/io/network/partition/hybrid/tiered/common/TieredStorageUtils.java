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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;

import java.nio.ByteBuffer;
import java.util.List;

/** Utils for reading from or writing to tiered storage. */
public class TieredStorageUtils {

    public static ByteBuffer[] generateBufferWithHeaders(
            List<Tuple2<Buffer, Integer>> bufferWithIndexes) {
        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * bufferWithIndexes.size()];

        for (int i = 0; i < bufferWithIndexes.size(); i++) {
            Buffer buffer = bufferWithIndexes.get(i).f0;
            setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
        }
        return bufferWithHeaders;
    }

    private static void setBufferWithHeader(
            Buffer buffer, ByteBuffer[] bufferWithHeaders, int index) {
        ByteBuffer header = BufferReaderWriterUtil.allocatedHeaderBuffer();
        BufferReaderWriterUtil.setByteChannelBufferHeader(buffer, header);

        bufferWithHeaders[index] = header;
        bufferWithHeaders[index + 1] = buffer.getNioBufferReadable();
    }
}
