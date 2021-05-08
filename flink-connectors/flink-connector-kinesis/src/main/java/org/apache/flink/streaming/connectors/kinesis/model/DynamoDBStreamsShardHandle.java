/*
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

package org.apache.flink.streaming.connectors.kinesis.model;

import com.amazonaws.services.kinesis.model.Shard;

/** DynamoDB streams shard handle format and utilities. */
public class DynamoDBStreamsShardHandle extends StreamShardHandle {
    public static final String SHARDID_PREFIX = "shardId-";
    public static final int SHARDID_PREFIX_LEN = SHARDID_PREFIX.length();

    public DynamoDBStreamsShardHandle(String streamName, Shard shard) {
        super(streamName, shard);
    }

    public static int compareShardIds(String firstShardId, String secondShardId) {
        if (!isValidShardId(firstShardId)) {
            throw new IllegalArgumentException(
                    String.format("The first shard id %s has invalid format.", firstShardId));
        } else if (!isValidShardId(secondShardId)) {
            throw new IllegalArgumentException(
                    String.format("The second shard id %s has invalid format.", secondShardId));
        }

        return firstShardId
                .substring(SHARDID_PREFIX_LEN)
                .compareTo(secondShardId.substring(SHARDID_PREFIX_LEN));
    }

    /**
     * Dynamodb streams shard ID is a char string ranging from 28 characters to 65 characters. (See
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Shard.html)
     *
     * <p>The shardId observed usually takes the format of: "shardId-00000001536805703746-69688cb1",
     * where "shardId-" is a prefix, followed by a 20-digit timestamp string and 0-36 or more
     * characters, separated by '-'. Following this format, it is expected the child shards created
     * during a re-sharding event have shardIds bigger than their parents.
     *
     * @param shardId shard Id
     * @return boolean indicate if the given shard Id is valid
     */
    public static boolean isValidShardId(String shardId) {
        return shardId == null ? false : shardId.matches("^shardId-\\d{20}-{0,1}\\w{0,36}");
    }
}
