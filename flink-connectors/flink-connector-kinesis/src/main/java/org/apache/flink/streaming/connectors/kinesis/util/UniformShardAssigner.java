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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.kinesis.KinesisShardAssigner;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;

import com.amazonaws.services.kinesis.model.HashKeyRange;

import java.math.BigInteger;

/**
 * A {@link KinesisShardAssigner} that maps Kinesis shard hash-key ranges to Flink subtasks. It
 * creates a more uniform distribution of shards across subtasks than {@link
 * org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher#DEFAULT_SHARD_ASSIGNER}
 * when the Kinesis records in the stream have hash keys that are uniformly distributed over all
 * possible hash keys, which is the case if records have randomly-generated partition keys. (This is
 * the same assumption made if you use the Kinesis UpdateShardCount operation with UNIFORM_SCALING.)
 */
@PublicEvolving
public class UniformShardAssigner implements KinesisShardAssigner {

    // BigInteger.TWO only available in Java versions > 8
    private static final BigInteger TWO = BigInteger.valueOf(2);

    // Kinesis data stream hash keys are < 2^128.
    private static final BigInteger HASH_KEY_BOUND = TWO.pow(128);

    @Override
    public int assign(StreamShardHandle streamShardHandle, int nSubtasks) {
        HashKeyRange range = streamShardHandle.getShard().getHashKeyRange();
        BigInteger hashKeyStart = new BigInteger(range.getStartingHashKey());
        BigInteger hashKeyEnd = new BigInteger(range.getEndingHashKey());
        BigInteger hashKeyMid = hashKeyStart.add(hashKeyEnd).divide(TWO);
        // index = hashKeyMid / HASH_KEY_BOUND * nSubtasks + stream-specific offset
        // The stream specific offset is added so that different streams will be less likely to be
        // distributed in the same way, even if they are sharded in the same way.
        // (The caller takes result modulo nSubtasks.)
        return hashKeyMid.multiply(BigInteger.valueOf(nSubtasks)).divide(HASH_KEY_BOUND).intValue()
                + streamShardHandle.getStreamName().hashCode();
    }
}
