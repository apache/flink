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
package org.apache.kafka.copied.common.requests;

import org.apache.kafka.copied.common.TopicPartition;
import org.apache.kafka.copied.common.protocol.ApiKeys;
import org.apache.kafka.copied.common.protocol.Errors;
import org.apache.kafka.copied.common.protocol.ProtoUtils;
import org.apache.kafka.copied.common.protocol.types.Schema;
import org.apache.kafka.copied.common.protocol.types.Struct;
import org.apache.kafka.copied.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListOffsetRequest extends AbstractRequest {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.LIST_OFFSETS.id);
    private static final String REPLICA_ID_KEY_NAME = "replica_id";
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String TIMESTAMP_KEY_NAME = "timestamp";
    private static final String MAX_NUM_OFFSETS_KEY_NAME = "max_num_offsets";

    private final int replicaId;
    private final Map<TopicPartition, PartitionData> offsetData;

    public static final class PartitionData {
        public final long timestamp;
        public final int maxNumOffsets;

        public PartitionData(long timestamp, int maxNumOffsets) {
            this.timestamp = timestamp;
            this.maxNumOffsets = maxNumOffsets;
        }
    }
    
    public ListOffsetRequest(Map<TopicPartition, PartitionData> offsetData) {
        this(-1, offsetData);
    }

    public ListOffsetRequest(int replicaId, Map<TopicPartition, PartitionData> offsetData) {
        super(new Struct(CURRENT_SCHEMA));
        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupDataByTopic(offsetData);

        struct.set(REPLICA_ID_KEY_NAME, replicaId);
        List<Struct> topicArray = new ArrayList<Struct>();
        for (Map.Entry<String, Map<Integer, PartitionData>> topicEntry: topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<Struct>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.getValue().entrySet()) {
                PartitionData offsetPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionData.set(TIMESTAMP_KEY_NAME, offsetPartitionData.timestamp);
                partitionData.set(MAX_NUM_OFFSETS_KEY_NAME, offsetPartitionData.maxNumOffsets);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS_KEY_NAME, topicArray.toArray());
        this.replicaId = replicaId;
        this.offsetData = offsetData;
    }

    public ListOffsetRequest(Struct struct) {
        super(struct);
        replicaId = struct.getInt(REPLICA_ID_KEY_NAME);
        offsetData = new HashMap<TopicPartition, PartitionData>();
        for (Object topicResponseObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                long timestamp = partitionResponse.getLong(TIMESTAMP_KEY_NAME);
                int maxNumOffsets = partitionResponse.getInt(MAX_NUM_OFFSETS_KEY_NAME);
                PartitionData partitionData = new PartitionData(timestamp, maxNumOffsets);
                offsetData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        Map<TopicPartition, ListOffsetResponse.PartitionData> responseData = new HashMap<TopicPartition, ListOffsetResponse.PartitionData>();

        for (Map.Entry<TopicPartition, PartitionData> entry: offsetData.entrySet()) {
            ListOffsetResponse.PartitionData partitionResponse = new ListOffsetResponse.PartitionData(Errors.forException(e).code(), new ArrayList<Long>());
            responseData.put(entry.getKey(), partitionResponse);
        }

        switch (versionId) {
            case 0:
                return new ListOffsetResponse(responseData);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.LIST_OFFSETS.id)));
        }
    }

    public int replicaId() {
        return replicaId;
    }

    public Map<TopicPartition, PartitionData> offsetData() {
        return offsetData;
    }

    public static ListOffsetRequest parse(ByteBuffer buffer, int versionId) {
        return new ListOffsetRequest(ProtoUtils.parseRequest(ApiKeys.LIST_OFFSETS.id, versionId, buffer));
    }

    public static ListOffsetRequest parse(ByteBuffer buffer) {
        return new ListOffsetRequest((Struct) CURRENT_SCHEMA.read(buffer));
    }
}
