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

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.consumer.ChannelStatePersister;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for logging actions that happened in the network stack for debugging purposes.
 *
 * <p>Action parameter typically includes class and method names.
 */
public class NetworkActionsLogger {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkActionsLogger.class);
    private static final boolean ENABLED = LOG.isTraceEnabled();
    private static final boolean INCLUDE_HASH = true;

    public static void traceInput(
            String action,
            Buffer buffer,
            String taskName,
            InputChannelInfo channelInfo,
            ChannelStatePersister channelStatePersister,
            int sequenceNumber) {
        if (ENABLED) {
            LOG.trace(
                    "[{}] {} {}, seq {}, {} @ {}",
                    taskName,
                    action,
                    buffer.toDebugString(INCLUDE_HASH),
                    sequenceNumber,
                    channelStatePersister,
                    channelInfo);
        }
    }

    public static void traceOutput(
            String action, Buffer buffer, String taskName, ResultSubpartitionInfo channelInfo) {
        if (ENABLED) {
            LOG.trace(
                    "[{}] {} {} @ {}",
                    taskName,
                    action,
                    buffer.toDebugString(INCLUDE_HASH),
                    channelInfo);
        }
    }

    public static void traceRecover(
            String action, Buffer buffer, String taskName, InputChannelInfo channelInfo) {
        if (ENABLED) {
            LOG.trace(
                    "[{}] {} {} @ {}",
                    taskName,
                    action,
                    buffer.toDebugString(INCLUDE_HASH),
                    channelInfo);
        }
    }

    public static void traceRecover(
            String action, BufferConsumer bufferConsumer, ResultSubpartitionInfo channelInfo) {
        if (ENABLED) {
            LOG.trace(
                    "{} {} @ {}", action, bufferConsumer.toDebugString(INCLUDE_HASH), channelInfo);
        }
    }

    public static void tracePersist(
            String action, Buffer buffer, Object channelInfo, long checkpointId) {
        if (ENABLED) {
            LOG.trace(
                    "{} {}, checkpoint {} @ {}",
                    action,
                    buffer.toDebugString(INCLUDE_HASH),
                    checkpointId,
                    channelInfo);
        }
    }
}
