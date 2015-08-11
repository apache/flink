/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.kafka_backport.common.requests;

import org.apache.flink.kafka_backport.common.protocol.Protocol;
import org.apache.flink.kafka_backport.common.protocol.types.Field;
import org.apache.flink.kafka_backport.common.protocol.types.Struct;

import java.nio.ByteBuffer;

// ----------------------------------------------------------------------------
//  This class is copied from the Apache Kafka project.
// 
//  The class is part of a "backport" of the new consumer API, in order to
//  give Flink access to its functionality until the API is properly released.
// 
//  This is a temporary workaround!
// ----------------------------------------------------------------------------

/**
 * A response header in the kafka protocol.
 */
public class ResponseHeader extends AbstractRequestResponse {

    private static final Field CORRELATION_KEY_FIELD = Protocol.RESPONSE_HEADER.get("correlation_id");

    private final int correlationId;

    public ResponseHeader(Struct header) {
        super(header);
        correlationId = struct.getInt(CORRELATION_KEY_FIELD);
    }

    public ResponseHeader(int correlationId) {
        super(new Struct(Protocol.RESPONSE_HEADER));
        struct.set(CORRELATION_KEY_FIELD, correlationId);
        this.correlationId = correlationId;
    }

    public int correlationId() {
        return correlationId;
    }

    public static ResponseHeader parse(ByteBuffer buffer) {
        return new ResponseHeader((Struct) Protocol.RESPONSE_HEADER.read(buffer));
    }

}
