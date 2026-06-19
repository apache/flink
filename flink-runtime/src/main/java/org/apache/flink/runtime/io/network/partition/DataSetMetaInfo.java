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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.OptionalInt;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.ToIntFunction;

/** Container for meta-data of a data set. */
public final class DataSetMetaInfo implements Serializable {
    private static final int UNKNOWN = -1;

    private final int numRegisteredPartitions;
    private final int numTotalPartitions;
    private final SortedMap<ResultPartitionID, ShuffleDescriptor>
            shuffleDescriptorsOrderByPartitionId =
                    new TreeMap<>(
                            Comparator.comparingInt(
                                    (ToIntFunction<? super ResultPartitionID> & Serializable)
                                            o -> o.getPartitionId().getPartitionNumber()));

    private DataSetMetaInfo(int numRegisteredPartitions, int numTotalPartitions) {
        this.numRegisteredPartitions = numRegisteredPartitions;
        this.numTotalPartitions = numTotalPartitions;
    }

    public OptionalInt getNumRegisteredPartitions() {
        return numRegisteredPartitions == UNKNOWN
                ? OptionalInt.empty()
                : OptionalInt.of(numRegisteredPartitions);
    }

    public int getNumTotalPartitions() {
        return numTotalPartitions;
    }

    public DataSetMetaInfo addShuffleDescriptors(
            Map<ResultPartitionID, ShuffleDescriptor> shuffleDescriptors) {
        this.shuffleDescriptorsOrderByPartitionId.putAll(shuffleDescriptors);
        return this;
    }

    public Map<ResultPartitionID, ShuffleDescriptor> getShuffleDescriptors() {
        return this.shuffleDescriptorsOrderByPartitionId;
    }

    static DataSetMetaInfo withoutNumRegisteredPartitions(int numTotalPartitions) {
        return new DataSetMetaInfo(UNKNOWN, numTotalPartitions);
    }

    @VisibleForTesting
    public static DataSetMetaInfo withNumRegisteredPartitions(
            int numRegisteredPartitions, int numTotalPartitions) {
        Preconditions.checkArgument(numRegisteredPartitions > 0);
        return new DataSetMetaInfo(numRegisteredPartitions, numTotalPartitions);
    }
}
