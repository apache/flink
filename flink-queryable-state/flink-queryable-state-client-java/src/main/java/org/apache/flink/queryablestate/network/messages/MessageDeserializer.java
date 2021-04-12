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

package org.apache.flink.queryablestate.network.messages;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * A utility used to deserialize a {@link MessageBody message}.
 *
 * @param <M> The type of the message to be deserialized. It has to extend {@link MessageBody}
 */
@Internal
public interface MessageDeserializer<M extends MessageBody> {

    /**
     * Deserializes a message contained in a byte buffer.
     *
     * @param buf the buffer containing the message.
     * @return The deserialized message.
     */
    M deserializeMessage(ByteBuf buf);
}
