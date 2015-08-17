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
import org.apache.flink.kafka_backport.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// ----------------------------------------------------------------------------
//  This class is copied from the Apache Kafka project.
// 
//  The class is part of a "backport" of the new consumer API, in order to
//  give Flink access to its functionality until the API is properly released.
// 
//  This is a temporary workaround!
// ----------------------------------------------------------------------------

public class JoinGroupResponse extends AbstractRequestResponse {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.JOIN_GROUP.id);
    private static final String ERROR_CODE_KEY_NAME = "error_code";

    /**
     * Possible error code:
     *
     * CONSUMER_COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR_FOR_CONSUMER (16)
     * INCONSISTENT_PARTITION_ASSIGNMENT_STRATEGY (23)
     * UNKNOWN_PARTITION_ASSIGNMENT_STRATEGY (24)
     * UNKNOWN_CONSUMER_ID (25)
     * INVALID_SESSION_TIMEOUT (26)
     */

    private static final String GENERATION_ID_KEY_NAME = "group_generation_id";
    private static final String CONSUMER_ID_KEY_NAME = "consumer_id";
    private static final String ASSIGNED_PARTITIONS_KEY_NAME = "assigned_partitions";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    public static final int UNKNOWN_GENERATION_ID = -1;
    public static final String UNKNOWN_CONSUMER_ID = "";

    private final short errorCode;
    private final int generationId;
    private final String consumerId;
    private final List<TopicPartition> assignedPartitions;

    public JoinGroupResponse(short errorCode, int generationId, String consumerId, List<TopicPartition> assignedPartitions) {
        super(new Struct(CURRENT_SCHEMA));

        Map<String, List<Integer>> partitionsByTopic = CollectionUtils.groupDataByTopic(assignedPartitions);

        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        struct.set(GENERATION_ID_KEY_NAME, generationId);
        struct.set(CONSUMER_ID_KEY_NAME, consumerId);
        List<Struct> topicArray = new ArrayList<Struct>();
        for (Map.Entry<String, List<Integer>> entries: partitionsByTopic.entrySet()) {
            Struct topicData = struct.instance(ASSIGNED_PARTITIONS_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, entries.getKey());
            topicData.set(PARTITIONS_KEY_NAME, entries.getValue().toArray());
            topicArray.add(topicData);
        }
        struct.set(ASSIGNED_PARTITIONS_KEY_NAME, topicArray.toArray());

        this.errorCode = errorCode;
        this.generationId = generationId;
        this.consumerId = consumerId;
        this.assignedPartitions = assignedPartitions;
    }

    public JoinGroupResponse(Struct struct) {
        super(struct);
        assignedPartitions = new ArrayList<TopicPartition>();
        for (Object topicDataObj : struct.getArray(ASSIGNED_PARTITIONS_KEY_NAME)) {
            Struct topicData = (Struct) topicDataObj;
            String topic = topicData.getString(TOPIC_KEY_NAME);
            for (Object partitionObj : topicData.getArray(PARTITIONS_KEY_NAME))
                assignedPartitions.add(new TopicPartition(topic, (Integer) partitionObj));
        }
        errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        generationId = struct.getInt(GENERATION_ID_KEY_NAME);
        consumerId = struct.getString(CONSUMER_ID_KEY_NAME);
    }

    public short errorCode() {
        return errorCode;
    }

    public int generationId() {
        return generationId;
    }

    public String consumerId() {
        return consumerId;
    }

    public List<TopicPartition> assignedPartitions() {
        return assignedPartitions;
    }

    public static JoinGroupResponse parse(ByteBuffer buffer) {
        return new JoinGroupResponse((Struct) CURRENT_SCHEMA.read(buffer));
    }
}