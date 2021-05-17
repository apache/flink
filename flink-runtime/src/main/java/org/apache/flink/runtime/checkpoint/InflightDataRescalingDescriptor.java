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

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

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
    public static final InflightDataRescalingDescriptor NO_RESCALE = new NoRescalingDescriptor();

    private static final long serialVersionUID = -3396674344669796295L;

    /** Set when several operator instances are merged into one. */
    private final int[] oldSubtaskIndexes;

    /**
     * Set when channels are merged because the connected operator has been rescaled for each
     * gate/partition.
     */
    private final RescaleMappings[] rescaledChannelsMappings;

    /** All channels where upstream duplicates data (only valid for downstream mappings). */
    private final Set<Integer> ambiguousSubtaskIndexes;

    public InflightDataRescalingDescriptor(
            int[] oldSubtaskIndexes,
            RescaleMappings[] rescaledChannelsMappings,
            Set<Integer> ambiguousSubtaskIndexes) {
        this.oldSubtaskIndexes = checkNotNull(oldSubtaskIndexes);
        this.rescaledChannelsMappings = checkNotNull(rescaledChannelsMappings);
        this.ambiguousSubtaskIndexes = checkNotNull(ambiguousSubtaskIndexes);
    }

    public int[] getOldSubtaskIndexes() {
        return oldSubtaskIndexes;
    }

    public RescaleMappings getChannelMapping(int gateOrPartitionIndex) {
        return rescaledChannelsMappings[gateOrPartitionIndex];
    }

    public boolean isAmbiguous(int oldSubtaskIndex) {
        return ambiguousSubtaskIndexes.contains(oldSubtaskIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InflightDataRescalingDescriptor that = (InflightDataRescalingDescriptor) o;
        return Arrays.equals(oldSubtaskIndexes, that.oldSubtaskIndexes)
                && Arrays.equals(rescaledChannelsMappings, that.rescaledChannelsMappings)
                && Objects.equals(ambiguousSubtaskIndexes, that.ambiguousSubtaskIndexes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(ambiguousSubtaskIndexes);
        result = 31 * result + Arrays.hashCode(oldSubtaskIndexes);
        result = 31 * result + Arrays.hashCode(rescaledChannelsMappings);
        return result;
    }

    @Override
    public String toString() {
        return "InflightDataRescalingDescriptor{"
                + "oldSubtaskIndexes="
                + Arrays.toString(oldSubtaskIndexes)
                + ", rescaledChannelsMappings="
                + Arrays.toString(rescaledChannelsMappings)
                + ", ambiguousSubtaskIndexes="
                + ambiguousSubtaskIndexes
                + '}';
    }

    private static class NoRescalingDescriptor extends InflightDataRescalingDescriptor {
        private static final long serialVersionUID = -5544173933105855751L;

        public NoRescalingDescriptor() {
            super(new int[0], new RescaleMappings[0], Collections.emptySet());
        }

        @Override
        public RescaleMappings getChannelMapping(int gateOrPartitionIndex) {
            return RescaleMappings.SYMMETRIC_IDENTITY;
        }

        private Object readResolve() throws ObjectStreamException {
            return NO_RESCALE;
        }

        @Override
        public String toString() {
            return "NoRescalingDescriptor";
        }
    }
}
