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

package org.apache.flink.runtime.checkpoint;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Captures ambiguous mappings of old channels to new channels.
 *
 * <p>For inputs, this mapping implies the following:
 * <li>
 *
 *     <ul>
 *       {@link #oldSubtaskIndexes} is set when there is a rescale on this task potentially leading
 *       to different key groups. Upstream task has a corresponding {@link
 *       #rescaledChannelsMappings} where it sends data over virtual channel while specifying the
 *       channel index in the VirtualChannelSelector. This subtask then demultiplexes over the
 *       virtual subtask index.
 * </ul>
 *
 * <ul>
 *   {@link #rescaledChannelsMappings} is set when there is a downscale of the upstream task.
 *   Upstream task has a corresponding {@link #oldSubtaskIndexes} where it sends data over virtual
 *   channel while specifying the subtask index in the VirtualChannelSelector. This subtask then
 *   demultiplexes over channel indexes.
 * </ul>
 *
 * <p>For outputs, it's vice-versa. The information must be kept in sync but they are used in
 * opposite ways for multiplexing/demultiplexing.
 *
 * <p>Note that in the common rescaling case both information is set and need to be simultaneously
 * used. If the input subtask subsumes the state of 3 old subtasks and a channel corresponds to 2
 * old channels, then there are 6 virtual channels to be demultiplexed.
 */
public class InflightDataRescalingDescriptor implements Serializable {
    public static final Set<Integer> NO_SUBTASKS = emptySet();
    public static final Map<Integer, RescaledChannelsMapping> NO_MAPPINGS = emptyMap();
    public static final InflightDataRescalingDescriptor NO_RESCALE =
            new InflightDataRescalingDescriptor(NO_SUBTASKS, NO_MAPPINGS);

    private static final long serialVersionUID = -3396674344669796295L;

    /** Set when several operator instances are merged into one. */
    private final Set<Integer> oldSubtaskIndexes;

    /**
     * Set when channels are merged because the connected operator has been rescaled for each
     * gate/partition.
     */
    private final Map<Integer, RescaledChannelsMapping> rescaledChannelsMappings;

    public InflightDataRescalingDescriptor(
            Set<Integer> oldSubtaskIndexes,
            Map<Integer, RescaledChannelsMapping> rescaledChannelsMappings) {
        this.oldSubtaskIndexes = checkNotNull(oldSubtaskIndexes);
        this.rescaledChannelsMappings = checkNotNull(rescaledChannelsMappings);
    }

    public int[] getOldSubtaskIndexes(int defaultSubtask) {
        return oldSubtaskIndexes.equals(NO_SUBTASKS)
                ? new int[] {defaultSubtask}
                : oldSubtaskIndexes.stream().mapToInt(Integer::intValue).toArray();
    }

    public RescaledChannelsMapping getChannelMapping(int gateOrPartitionIndex) {
        return rescaledChannelsMappings.getOrDefault(
                gateOrPartitionIndex, RescaledChannelsMapping.NO_CHANNEL_MAPPING);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final InflightDataRescalingDescriptor that = (InflightDataRescalingDescriptor) o;
        return oldSubtaskIndexes.equals(that.oldSubtaskIndexes)
                && rescaledChannelsMappings.equals(that.rescaledChannelsMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oldSubtaskIndexes, rescaledChannelsMappings);
    }
}
