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

package org.apache.flink.runtime.state;

/** A {@link KeyGroupsStateHandle} that describes a savepoint in the unified format. */
public class KeyGroupsSavepointStateHandle extends KeyGroupsStateHandle
        implements SavepointKeyedStateHandle {

    private static final long serialVersionUID = 1L;

    /**
     * @param groupRangeOffsets range of key-group ids that in the state of this handle
     * @param streamStateHandle handle to the actual state of the key-groups
     */
    public KeyGroupsSavepointStateHandle(
            KeyGroupRangeOffsets groupRangeOffsets, StreamStateHandle streamStateHandle) {
        super(groupRangeOffsets, streamStateHandle);
    }

    /**
     * @param keyGroupRange a key group range to intersect.
     * @return key-group state over a range that is the intersection between this handle's key-group
     *     range and the provided key-group range.
     */
    @Override
    public KeyGroupsStateHandle getIntersection(KeyGroupRange keyGroupRange) {
        KeyGroupRangeOffsets offsets = getGroupRangeOffsets().getIntersection(keyGroupRange);
        if (offsets.getKeyGroupRange().getNumberOfKeyGroups() <= 0) {
            return null;
        }
        return new KeyGroupsSavepointStateHandle(offsets, getDelegateStateHandle());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof KeyGroupsSavepointStateHandle)) {
            return false;
        }

        return super.equals(o);
    }

    @Override
    public String toString() {
        return "KeyGroupsSavepointStateHandle{"
                + "groupRangeOffsets="
                + getGroupRangeOffsets()
                + ", stateHandle="
                + getDelegateStateHandle()
                + '}';
    }
}
