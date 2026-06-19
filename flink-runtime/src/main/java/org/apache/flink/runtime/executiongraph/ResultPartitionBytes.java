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

package org.apache.flink.runtime.executiongraph;

import java.io.Serializable;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** This class represents a snapshot of the result partition bytes metrics. */
public class ResultPartitionBytes implements Serializable {

    private final long[] subpartitionBytes;

    public ResultPartitionBytes(long[] subpartitionBytes) {
        this.subpartitionBytes = checkNotNull(subpartitionBytes);
    }

    public long[] getSubpartitionBytes() {
        return subpartitionBytes;
    }

    /** Merge all {@link ResultPartitionBytes} by sum up them per-subpartition. */
    public static ResultPartitionBytes mergeAll(List<ResultPartitionBytes> partitions) {
        checkArgument(!partitions.isEmpty());
        int expectedLength = partitions.get(0).getSubpartitionBytes().length;
        for (ResultPartitionBytes resultPartitionByte : partitions) {
            if (resultPartitionByte.getSubpartitionBytes().length != expectedLength) {
                throw new IllegalArgumentException(
                        "only all ResultPartitionBytes with the same length can be merged");
            }
        }
        long[] mergedSubpartitionBytes = new long[expectedLength];
        for (int i = 0; i < expectedLength; i++) {
            for (ResultPartitionBytes resultPartitionByte : partitions) {
                mergedSubpartitionBytes[i] += resultPartitionByte.getSubpartitionBytes()[i];
            }
        }
        return new ResultPartitionBytes(mergedSubpartitionBytes);
    }
}
