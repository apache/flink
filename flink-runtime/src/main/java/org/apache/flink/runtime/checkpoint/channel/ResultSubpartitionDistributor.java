/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** The distributor for channel state of result subpartition. */
public class ResultSubpartitionDistributor {

    private final InflightDataRescalingDescriptor channelMapping;

    private final Map<ResultSubpartitionInfo, List<ResultSubpartitionInfo>> rescaledChannels =
            new HashMap<>();
    private final Map<Integer, RescaleMappings> oldToNewMappings = new HashMap<>();

    public ResultSubpartitionDistributor(InflightDataRescalingDescriptor channelMapping) {
        this.channelMapping = channelMapping;
    }

    public List<ResultSubpartitionInfo> getMappedSubpartitions(
            ResultSubpartitionInfo subpartitionInfo) {
        return rescaledChannels.computeIfAbsent(subpartitionInfo, this::calculateMapping);
    }

    private List<ResultSubpartitionInfo> calculateMapping(ResultSubpartitionInfo info) {
        final RescaleMappings oldToNewMapping =
                oldToNewMappings.computeIfAbsent(
                        info.getPartitionIdx(),
                        idx -> channelMapping.getChannelMapping(idx).invert());
        final List<ResultSubpartitionInfo> subpartitions =
                Arrays.stream(oldToNewMapping.getMappedIndexes(info.getSubPartitionIdx()))
                        .mapToObj(
                                newIndexes ->
                                        getSubpartitionInfo(info.getPartitionIdx(), newIndexes))
                        .collect(Collectors.toList());
        if (subpartitions.isEmpty()) {
            throw new IllegalStateException(
                    "Recovered a buffer from old "
                            + info
                            + " that has no mapping in "
                            + channelMapping.getChannelMapping(info.getPartitionIdx()));
        }
        return subpartitions;
    }

    ResultSubpartitionInfo getSubpartitionInfo(int partitionIndex, int subPartitionIdx) {
        return new ResultSubpartitionInfo(partitionIndex, subPartitionIdx);
    }
}
