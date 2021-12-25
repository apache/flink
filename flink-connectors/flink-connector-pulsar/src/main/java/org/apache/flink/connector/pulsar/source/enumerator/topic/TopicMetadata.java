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

package org.apache.flink.connector.pulsar.source.enumerator.topic;

import org.apache.flink.annotation.PublicEvolving;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.pulsar.common.partition.PartitionedTopicMetadata.NON_PARTITIONED;

/** The pojo class for pulsar topic metadata information. */
@PublicEvolving
public final class TopicMetadata {

    /**
     * The name of the topic, it would be a {@link TopicNameUtils#topicName(String)} which don't
     * contain partition information.
     */
    private final String name;

    /** The size for a partitioned topic. It would be zero for non-partitioned topic. */
    private final int partitionSize;

    public TopicMetadata(String name, int partitionSize) {
        checkArgument(partitionSize >= 0);

        this.name = name;
        this.partitionSize = partitionSize;
    }

    public String getName() {
        return name;
    }

    public boolean isPartitioned() {
        return partitionSize != NON_PARTITIONED;
    }

    public int getPartitionSize() {
        return partitionSize;
    }
}
