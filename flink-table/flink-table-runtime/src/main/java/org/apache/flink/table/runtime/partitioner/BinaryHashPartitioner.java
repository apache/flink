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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.util.MathUtils;

import java.util.Arrays;

/** Hash partitioner for {@link BinaryRowData}. */
public class BinaryHashPartitioner extends StreamPartitioner<RowData> {

    private GeneratedHashFunction genHashFunc;

    private transient HashFunction hashFunc;
    private String[] hashFieldNames;

    public BinaryHashPartitioner(GeneratedHashFunction genHashFunc, String[] hashFieldNames) {
        this.genHashFunc = genHashFunc;
        this.hashFieldNames = hashFieldNames;
    }

    @Override
    public StreamPartitioner<RowData> copy() {
        return this;
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<RowData>> record) {
        return MathUtils.murmurHash(getHashFunc().hashCode(record.getInstance().getValue()))
                % numberOfChannels;
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.FULL;
    }

    private HashFunction getHashFunc() {
        if (hashFunc == null) {
            try {
                hashFunc = genHashFunc.newInstance(Thread.currentThread().getContextClassLoader());
                genHashFunc = null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return hashFunc;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public String toString() {
        return "HASH" + Arrays.toString(hashFieldNames);
    }
}
