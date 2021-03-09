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

package org.apache.flink.queryablestate.messages;

import org.apache.flink.annotation.Internal;
import org.apache.flink.queryablestate.network.messages.MessageBody;
import org.apache.flink.queryablestate.network.messages.MessageDeserializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * The response containing the (serialized) state sent by the {@code State Server} to the {@code
 * Client Proxy}, and then forwarded by the proxy to the original {@link
 * org.apache.flink.queryablestate.client.QueryableStateClient Queryable State Client}.
 */
@Internal
public class KvStateResponse extends MessageBody {

    private final byte[] content;

    public KvStateResponse(final byte[] content) {
        this.content = Preconditions.checkNotNull(content);
    }

    public byte[] getContent() {
        return content;
    }

    @Override
    public byte[] serialize() {
        final int size = Integer.BYTES + content.length;
        return ByteBuffer.allocate(size).putInt(content.length).put(content).array();
    }

    /** A {@link MessageDeserializer deserializer} for {@link KvStateResponseDeserializer}. */
    public static class KvStateResponseDeserializer
            implements MessageDeserializer<KvStateResponse> {

        @Override
        public KvStateResponse deserializeMessage(ByteBuf buf) {
            int length = buf.readInt();
            Preconditions.checkArgument(
                    length >= 0,
                    "Negative length for state content. "
                            + "This indicates a serialization error.");
            byte[] content = new byte[length];
            buf.readBytes(content);

            return new KvStateResponse(content);
        }
    }
}
