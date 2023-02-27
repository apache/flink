/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * The default {@link InternalKeyContext} implementation.
 *
 * @param <K> Type of the key.
 */
public class InternalKeyContextImpl<K> implements InternalKeyContext<K> {
    /** Range of key-groups for which this backend is responsible. */
    private final KeyGroupRange keyGroupRange;
    /** The number of key-groups aka max parallelism. */
    private final int numberOfKeyGroups;

    /** The currently active key. */
    private K currentKey;
    /** The key group of the currently active key. */
    private int currentKeyGroupIndex;

    public InternalKeyContextImpl(
            @Nonnull KeyGroupRange keyGroupRange, @Nonnegative int numberOfKeyGroups) {
        this.keyGroupRange = keyGroupRange;
        this.numberOfKeyGroups = numberOfKeyGroups;
    }

    @Override
    public K getCurrentKey() {
        return currentKey;
    }

    @Override
    public int getCurrentKeyGroupIndex() {
        return currentKeyGroupIndex;
    }

    @Override
    public int getNumberOfKeyGroups() {
        return numberOfKeyGroups;
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Override
    public void setCurrentKey(@Nonnull K currentKey) {
        this.currentKey = currentKey;
    }

    @Override
    public void setCurrentKeyGroupIndex(int currentKeyGroupIndex) {
        if (!keyGroupRange.contains(currentKeyGroupIndex)) {
            throw KeyGroupRangeOffsets.newIllegalKeyGroupException(
                    currentKeyGroupIndex, keyGroupRange);
        }
        this.currentKeyGroupIndex = currentKeyGroupIndex;
    }
}
