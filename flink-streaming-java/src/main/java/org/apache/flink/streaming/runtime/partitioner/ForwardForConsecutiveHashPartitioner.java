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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * If there are multiple consecutive and the same hash shuffles, SQL planner will change them except
 * the first one to use forward partitioner, so that these operators can be chained to reduce
 * unnecessary shuffles.
 *
 * <pre>{@code
 * A --[hash]--> B --[hash]--> C
 *            |
 *            V
 * A --[hash]--> B --[forward]--> C
 *
 * }</pre>
 *
 * <p>However, sometimes the consecutive hash operators are not chained (e.g. multiple inputs), and
 * this kind of forward partitioners will turn into forward job edges. These forward edges still
 * have the consecutive hash assumption, so that they cannot be changed into rescale/rebalance
 * edges, otherwise it can lead to incorrect results. This prevents the adaptive batch scheduler
 * from determining parallelism for other forward edge downstream job vertices(see FLINK-25046).
 *
 * <p>To solve it, we introduce the {@link ForwardForConsecutiveHashPartitioner}. When SQL planner
 * optimizes the case of multiple consecutive and the same hash shuffles, it should use this
 * partitioner, and then the runtime framework will change it to forward/hash after the operator
 * chain creation.
 *
 * <pre>{@code
 * A --[hash]--> B --[hash]--> C
 *            |
 *            V
 * A --[hash]--> B --[ForwardForConsecutiveHash]--> C
 *
 * }</pre>
 *
 * <p>This partitioner will be converted to following partitioners after the operator chain
 * creation:
 *
 * <p>1. Be converted to {@link ForwardPartitioner} if this partitioner is intra-chain.
 *
 * <p>2. Be converted to {@link ForwardForConsecutiveHashPartitioner#hashPartitioner} if this
 * partitioner is inter-chain.
 *
 * <p>This partitioner should only be used for SQL Batch jobs and when using AdaptiveBatchScheduler.
 *
 * @param <T> Type of the elements in the Stream
 */
@Internal
public class ForwardForConsecutiveHashPartitioner<T> extends ForwardPartitioner<T> {

    private final StreamPartitioner<T> hashPartitioner;

    /**
     * Create a new ForwardForConsecutiveHashPartitioner.
     *
     * @param hashPartitioner the HashPartitioner
     */
    public ForwardForConsecutiveHashPartitioner(StreamPartitioner<T> hashPartitioner) {
        this.hashPartitioner = hashPartitioner;
    }

    @Override
    public StreamPartitioner<T> copy() {
        throw new RuntimeException(
                "ForwardForConsecutiveHashPartitioner is a intermediate partitioner in optimization phase, "
                        + "should be converted to a ForwardPartitioner and its underlying hashPartitioner at runtime.");
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        throw new RuntimeException(
                "ForwardForConsecutiveHashPartitioner is a intermediate partitioner in optimization phase, "
                        + "should be converted to a ForwardPartitioner and its underlying hashPartitioner at runtime.");
    }

    @Override
    public boolean isPointwise() {
        // will be used in StreamGraphGenerator#shouldDisableUnalignedCheckpointing, so can't throw
        // exception.
        return true;
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        throw new RuntimeException(
                "ForwardForConsecutiveHashPartitioner is a intermediate partitioner in optimization phase, "
                        + "should be converted to a ForwardPartitioner and its underlying hashPartitioner at runtime.");
    }

    public StreamPartitioner<T> getHashPartitioner() {
        return hashPartitioner;
    }
}
