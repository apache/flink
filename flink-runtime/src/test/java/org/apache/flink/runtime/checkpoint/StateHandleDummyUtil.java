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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class StateHandleDummyUtil {

    /**
     * Creates a new test {@link OperatorStreamStateHandle} with a given number of randomly created
     * named states.
     */
    public static OperatorStateHandle createNewOperatorStateHandle(
            int numNamedStates, Random random) {
        Map<String, OperatorStateHandle.StateMetaInfo> operatorStateMetaData =
                new HashMap<>(numNamedStates);
        long off = 0;
        for (int i = 0; i < numNamedStates; ++i) {
            long[] offsets = new long[4];
            for (int o = 0; o < offsets.length; ++o) {
                offsets[o] = off++;
            }
            OperatorStateHandle.StateMetaInfo metaInfo =
                    new OperatorStateHandle.StateMetaInfo(
                            offsets, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);
            operatorStateMetaData.put(String.valueOf(UUID.randomUUID()), metaInfo);
        }
        return new OperatorStreamStateHandle(
                operatorStateMetaData, createStreamStateHandle(numNamedStates, random));
    }

    private static ByteStreamStateHandle createStreamStateHandle(
            int numNamedStates, Random random) {
        byte[] streamData = new byte[numNamedStates * 4];
        random.nextBytes(streamData);
        return new ByteStreamStateHandle(String.valueOf(UUID.randomUUID()), streamData);
    }

    /** Creates a new test {@link KeyedStateHandle} for the given key-group. */
    public static KeyedStateHandle createNewKeyedStateHandle(KeyGroupRange keyGroupRange) {
        return new DummyKeyedStateHandle(keyGroupRange);
    }

    /** Creates a deep copy of the given {@link OperatorStreamStateHandle}. */
    public static OperatorStateHandle deepDummyCopy(OperatorStateHandle original) {

        if (original == null) {
            return null;
        }

        ByteStreamStateHandle stateHandleCopy =
                cloneByteStreamStateHandle(
                        (ByteStreamStateHandle) original.getDelegateStateHandle());
        Map<String, OperatorStateHandle.StateMetaInfo> offsets =
                original.getStateNameToPartitionOffsets();
        Map<String, OperatorStateHandle.StateMetaInfo> offsetsCopy = new HashMap<>(offsets.size());

        for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> entry : offsets.entrySet()) {
            OperatorStateHandle.StateMetaInfo metaInfo = entry.getValue();
            OperatorStateHandle.StateMetaInfo metaInfoCopy =
                    new OperatorStateHandle.StateMetaInfo(
                            metaInfo.getOffsets(), metaInfo.getDistributionMode());
            offsetsCopy.put(String.valueOf(entry.getKey()), metaInfoCopy);
        }
        return new OperatorStreamStateHandle(offsetsCopy, stateHandleCopy);
    }

    /** Creates deep copy of the given {@link KeyedStateHandle}. */
    public static KeyedStateHandle deepDummyCopy(KeyedStateHandle original) {

        if (original == null) {
            return null;
        }

        KeyGroupRange keyGroupRange = original.getKeyGroupRange();
        return new DummyKeyedStateHandle(
                new KeyGroupRange(
                        keyGroupRange.getStartKeyGroup(), keyGroupRange.getEndKeyGroup()));
    }

    public static InputChannelStateHandle deepDummyCopy(InputChannelStateHandle original) {
        if (original == null) {
            return null;
        }
        return new InputChannelStateHandle(
                new InputChannelInfo(
                        original.getInfo().getGateIdx(), original.getInfo().getInputChannelIdx()),
                cloneByteStreamStateHandle((ByteStreamStateHandle) original.getDelegate()),
                new ArrayList<>(original.getOffsets()));
    }

    public static ResultSubpartitionStateHandle deepDummyCopy(
            ResultSubpartitionStateHandle original) {
        if (original == null) {
            return null;
        }
        return new ResultSubpartitionStateHandle(
                new ResultSubpartitionInfo(
                        original.getInfo().getPartitionIdx(),
                        original.getInfo().getSubPartitionIdx()),
                cloneByteStreamStateHandle((ByteStreamStateHandle) original.getDelegate()),
                new ArrayList<>(original.getOffsets()));
    }

    private static ByteStreamStateHandle cloneByteStreamStateHandle(
            ByteStreamStateHandle delegate) {
        return new ByteStreamStateHandle(
                String.valueOf(delegate.getHandleName()), delegate.getData().clone());
    }

    public static InputChannelStateHandle createNewInputChannelStateHandle(
            int numNamedStates, Random random) {
        return new InputChannelStateHandle(
                new InputChannelInfo(random.nextInt(), random.nextInt()),
                createStreamStateHandle(numNamedStates, random),
                genOffsets(numNamedStates, random));
    }

    public static ResultSubpartitionStateHandle createNewResultSubpartitionStateHandle(
            int i, Random random) {
        return new ResultSubpartitionStateHandle(
                new ResultSubpartitionInfo(random.nextInt(), random.nextInt()),
                createStreamStateHandle(i, random),
                genOffsets(i, random));
    }

    private static ArrayList<Long> genOffsets(int size, Random random) {
        final ArrayList<Long> offsets = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            offsets.add(random.nextLong());
        }
        return offsets;
    }

    /** KeyedStateHandle that only holds a key-group information. */
    private static class DummyKeyedStateHandle implements KeyedStateHandle {

        private static final long serialVersionUID = 1L;

        private final KeyGroupRange keyGroupRange;

        private DummyKeyedStateHandle(KeyGroupRange keyGroupRange) {
            this.keyGroupRange = keyGroupRange;
        }

        @Override
        public KeyGroupRange getKeyGroupRange() {
            return keyGroupRange;
        }

        @Override
        public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
            return new DummyKeyedStateHandle(this.keyGroupRange.getIntersection(keyGroupRange));
        }

        @Override
        public void registerSharedStates(SharedStateRegistry stateRegistry) {}

        @Override
        public void discardState() throws Exception {}

        @Override
        public long getStateSize() {
            return 0L;
        }
    }
}
