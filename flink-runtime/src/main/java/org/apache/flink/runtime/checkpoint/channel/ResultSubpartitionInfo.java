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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.Objects;

/**
 * Identifies {@link org.apache.flink.runtime.io.network.partition.ResultSubpartition
 * ResultSubpartition} in a given subtask. Note that {@link
 * org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID IntermediateResultPartitionID}
 * can not be used because it: a) identifies the whole {@link
 * org.apache.flink.runtime.io.network.partition.ResultPartition ResultPartition} b) is generated
 * randomly.
 */
@Internal
public class ResultSubpartitionInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int partitionIdx;
    private final int subPartitionIdx;

    public ResultSubpartitionInfo(int partitionIdx, int subPartitionIdx) {
        this.partitionIdx = partitionIdx;
        this.subPartitionIdx = subPartitionIdx;
    }

    public int getPartitionIdx() {
        return partitionIdx;
    }

    public int getSubPartitionIdx() {
        return subPartitionIdx;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ResultSubpartitionInfo that = (ResultSubpartitionInfo) o;
        return partitionIdx == that.partitionIdx && subPartitionIdx == that.subPartitionIdx;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionIdx, subPartitionIdx);
    }

    @Override
    public String toString() {
        return "ResultSubpartitionInfo{"
                + "partitionIdx="
                + partitionIdx
                + ", subPartitionIdx="
                + subPartitionIdx
                + '}';
    }
}
