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
package org.apache.flink.kafka_backport.common.requests;

import org.apache.flink.kafka_backport.common.protocol.ProtoUtils;
import org.apache.flink.kafka_backport.common.protocol.types.Schema;
import org.apache.flink.kafka_backport.common.protocol.types.Struct;
import org.apache.flink.kafka_backport.common.TopicPartition;
import org.apache.flink.kafka_backport.common.protocol.ApiKeys;
import org.apache.flink.kafka_backport.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// ----------------------------------------------------------------------------
//  This class is copied from the Apache Kafka project.
// 
//  The class is part of a "backport" of the new consumer API, in order to
//  give Flink access to its functionality until the API is properly released.
// 
//  This is a temporary workaround!
// ----------------------------------------------------------------------------

public class JoinGroupRequest extends AbstractRequest {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.JOIN_GROUP.id);
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String SESSION_TIMEOUT_KEY_NAME = "session_timeout";
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String CONSUMER_ID_KEY_NAME = "consumer_id";
    private static final String STRATEGY_KEY_NAME = "partition_assignment_strategy";

    public static final String UNKNOWN_CONSUMER_ID = "";

    private final String groupId;
    private final int sessionTimeout;
    private final List<String> topics;
    private final String consumerId;
    private final String strategy;

    public JoinGroupRequest(String groupId, int sessionTimeout, List<String> topics, String consumerId, String strategy) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(GROUP_ID_KEY_NAME, groupId);
        struct.set(SESSION_TIMEOUT_KEY_NAME, sessionTimeout);
        struct.set(TOPICS_KEY_NAME, topics.toArray());
        struct.set(CONSUMER_ID_KEY_NAME, consumerId);
        struct.set(STRATEGY_KEY_NAME, strategy);
        this.groupId = groupId;
        this.sessionTimeout = sessionTimeout;
        this.topics = topics;
        this.consumerId = consumerId;
        this.strategy = strategy;
    }

    public JoinGroupRequest(Struct struct) {
        super(struct);
        groupId = struct.getString(GROUP_ID_KEY_NAME);
        sessionTimeout = struct.getInt(SESSION_TIMEOUT_KEY_NAME);
        Object[] topicsArray = struct.getArray(TOPICS_KEY_NAME);
        topics = new ArrayList<String>();
        for (Object topic: topicsArray)
            topics.add((String) topic);
        consumerId = struct.getString(CONSUMER_ID_KEY_NAME);
        strategy = struct.getString(STRATEGY_KEY_NAME);
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        switch (versionId) {
            case 0:
                return new JoinGroupResponse(
                        Errors.forException(e).code(),
                        JoinGroupResponse.UNKNOWN_GENERATION_ID,
                        JoinGroupResponse.UNKNOWN_CONSUMER_ID,
                        Collections.<TopicPartition>emptyList());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.JOIN_GROUP.id)));
        }
    }

    public String groupId() {
        return groupId;
    }

    public int sessionTimeout() {
        return sessionTimeout;
    }

    public List<String> topics() {
        return topics;
    }

    public String consumerId() {
        return consumerId;
    }

    public String strategy() {
        return strategy;
    }

    public static JoinGroupRequest parse(ByteBuffer buffer, int versionId) {
        return new JoinGroupRequest(ProtoUtils.parseRequest(ApiKeys.JOIN_GROUP.id, versionId, buffer));
    }

    public static JoinGroupRequest parse(ByteBuffer buffer) {
        return new JoinGroupRequest((Struct) CURRENT_SCHEMA.read(buffer));
    }
}
