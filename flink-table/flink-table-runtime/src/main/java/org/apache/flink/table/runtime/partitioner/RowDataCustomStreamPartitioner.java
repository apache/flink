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

package org.apache.flink.table.runtime.partitioner;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.source.abilities.SupportsLookupCustomShuffle.InputDataPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.util.Preconditions;

/** The partitioner used to partition row data by the connector's custom logic. */
public class RowDataCustomStreamPartitioner extends StreamPartitioner<RowData> {

    private final InputDataPartitioner partitioner;

    private final RowDataKeySelector keySelector;

    public RowDataCustomStreamPartitioner(
            InputDataPartitioner partitioner, RowDataKeySelector keySelector) {
        this.partitioner = partitioner;
        this.keySelector = keySelector;
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<RowData>> record) {
        RowData key;
        try {
            key = keySelector.getKey(record.getInstance().getValue());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not extract key from " + record.getInstance().getValue(), e);
        }
        int partition = partitioner.partition(key, numberOfChannels);
        Preconditions.checkState(partition < numberOfChannels);
        return partition;
    }

    @Override
    public StreamPartitioner<RowData> copy() {
        return this;
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.ARBITRARY;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public String toString() {
        return "CUSTOM";
    }
}
