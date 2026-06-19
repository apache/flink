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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ExecutionAttemptID}. */
class ExecutionAttemptIDTest {
    private static final NettyBufferPool ALLOCATOR = new NettyBufferPool(1);

    @Test
    void testByteBufWriteAndRead() {
        final ExecutionAttemptID executionAttemptID =
                new ExecutionAttemptID(
                        new ExecutionGraphID(), new ExecutionVertexID(new JobVertexID(), 123), 456);
        final int byteBufLen = ExecutionAttemptID.getByteBufLength();
        final ByteBuf byteBuf = ALLOCATOR.directBuffer(byteBufLen, byteBufLen);
        executionAttemptID.writeTo(byteBuf);

        assertThat(byteBuf.writerIndex()).isEqualTo(ExecutionAttemptID.getByteBufLength());
        assertThat(ExecutionAttemptID.fromByteBuf(byteBuf)).isEqualTo(executionAttemptID);
    }
}
