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
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * Partitioner that selects the channel with a user defined partitioner function on a key.
 *
 * @param <K> Type of the key
 * @param <T> Type of the data
 */
@Internal
public class CustomPartitionerWrapper<K, T> extends StreamPartitioner<T> {
    private static final long serialVersionUID = 1L;

    Partitioner<K> partitioner;
    KeySelector<T, K> keySelector;

    public CustomPartitionerWrapper(Partitioner<K> partitioner, KeySelector<T, K> keySelector) {
        this.partitioner = partitioner;
        this.keySelector = keySelector;
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        K key;
        try {
            key = keySelector.getKey(record.getInstance().getValue());
        } catch (Exception e) {
            throw new RuntimeException("Could not extract key from " + record.getInstance(), e);
        }

        return partitioner.partition(key, numberOfChannels);
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        // fully rely on filtering downstream
        // note that custom partitioners are not officially supported - the user has to force
        // rescaling in that case, we assume that the custom partitioner is deterministic
        return SubtaskStateMapper.FULL;
    }

    @Override
    public StreamPartitioner<T> copy() {
        try {
            // clone partitioner to ensure that any internal state is also replicated
            return new CustomPartitionerWrapper<>(
                    InstantiationUtil.clone(partitioner), keySelector);
        } catch (ClassNotFoundException | IOException e) {
            throw new IllegalStateException("Cannot clone custom partitioner", e);
        }
    }

    @Override
    public String toString() {
        return "CUSTOM";
    }
}
