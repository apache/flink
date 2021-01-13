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

package org.apache.flink.runtime.io.network.logger;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkState;

/** Utility class for logging actions that happened in the network stack for debugging purposes. */
public class NetworkActionsLogger {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkActionsLogger.class);

    private static final boolean ENABLED = LOG.isTraceEnabled();
    private static final boolean INCLUDE_HASH = true;

    public static void log(Class<?> clazz, String action, Buffer buffer) {
        if (ENABLED) {
            LOG.trace("{}#{} buffer = [{}]", clazz.getSimpleName(), action, toPrettyString(buffer));
        }
    }

    public static void log(Class<?> clazz, String action, BufferConsumer bufferConsumer) {
        if (ENABLED) {
            Buffer buffer = null;
            try (BufferConsumer copiedBufferConsumer = bufferConsumer.copy()) {
                buffer = copiedBufferConsumer.build();
                log(clazz, action, buffer);
                checkState(copiedBufferConsumer.isFinished());
            } finally {
                if (buffer != null) {
                    buffer.recycleBuffer();
                }
            }
        }
    }

    private static String toPrettyString(Buffer buffer) {
        StringBuilder prettyString = new StringBuilder("size=").append(buffer.getSize());
        if (INCLUDE_HASH) {
            byte[] bytes = new byte[buffer.getSize()];
            buffer.readOnlySlice().asByteBuf().readBytes(bytes);
            prettyString.append(", hash=").append(Arrays.hashCode(bytes));
        }
        return prettyString.toString();
    }
}
