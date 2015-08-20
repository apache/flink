/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.copied.common.requests;

import org.apache.kafka.copied.common.Node;
import org.apache.kafka.copied.common.protocol.ApiKeys;
import org.apache.kafka.copied.common.protocol.Errors;
import org.apache.kafka.copied.common.protocol.ProtoUtils;
import org.apache.kafka.copied.common.protocol.types.Schema;
import org.apache.kafka.copied.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class ConsumerMetadataRequest extends AbstractRequest {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.CONSUMER_METADATA.id);
    private static final String GROUP_ID_KEY_NAME = "group_id";

    private final String groupId;

    public ConsumerMetadataRequest(String groupId) {
        super(new Struct(CURRENT_SCHEMA));

        struct.set(GROUP_ID_KEY_NAME, groupId);
        this.groupId = groupId;
    }

    public ConsumerMetadataRequest(Struct struct) {
        super(struct);
        groupId = struct.getString(GROUP_ID_KEY_NAME);
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        switch (versionId) {
            case 0:
                return new ConsumerMetadataResponse(Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code(), Node.noNode());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.CONSUMER_METADATA.id)));
        }
    }

    public String groupId() {
        return groupId;
    }

    public static ConsumerMetadataRequest parse(ByteBuffer buffer, int versionId) {
        return new ConsumerMetadataRequest(ProtoUtils.parseRequest(ApiKeys.CONSUMER_METADATA.id, versionId, buffer));
    }

    public static ConsumerMetadataRequest parse(ByteBuffer buffer) {
        return new ConsumerMetadataRequest((Struct) CURRENT_SCHEMA.read(buffer));
    }
}
