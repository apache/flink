/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.kafka_backport.common.requests;

import org.apache.flink.kafka_backport.common.protocol.types.Struct;
import org.apache.flink.kafka_backport.common.protocol.ApiKeys;

import java.nio.ByteBuffer;

// ----------------------------------------------------------------------------
//  This class is copied from the Apache Kafka project.
// 
//  The class is part of a "backport" of the new consumer API, in order to
//  give Flink access to its functionality until the API is properly released.
// 
//  This is a temporary workaround!
// ----------------------------------------------------------------------------

public abstract class AbstractRequest extends AbstractRequestResponse {

    public AbstractRequest(Struct struct) {
        super(struct);
    }

    /**
     * Get an error response for a request for a given api version
     */
    public abstract AbstractRequestResponse getErrorResponse(int versionId, Throwable e);

    /**
     * Factory method for getting a request object based on ApiKey ID and a buffer
     */
    public static AbstractRequest getRequest(int requestId, int versionId, ByteBuffer buffer) {
        switch (ApiKeys.forId(requestId)) {
            case PRODUCE:
                return ProduceRequest.parse(buffer, versionId);
            case FETCH:
                return FetchRequest.parse(buffer, versionId);
            case LIST_OFFSETS:
                return ListOffsetRequest.parse(buffer, versionId);
            case METADATA:
                return MetadataRequest.parse(buffer, versionId);
            case OFFSET_COMMIT:
                return OffsetCommitRequest.parse(buffer, versionId);
            case OFFSET_FETCH:
                return OffsetFetchRequest.parse(buffer, versionId);
            case CONSUMER_METADATA:
                return ConsumerMetadataRequest.parse(buffer, versionId);
            case JOIN_GROUP:
                return JoinGroupRequest.parse(buffer, versionId);
            case HEARTBEAT:
                return HeartbeatRequest.parse(buffer, versionId);
            default:
                return null;
        }
    }
}