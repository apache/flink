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

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.mocks.MockSource;

import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HybridSourceSplitSerializer}. */
public class HybridSourceSplitSerializerTest {

    @ParameterizedTest(name = "isFinished = {0}")
    @ValueSource(booleans = {true, false})
    public void testSerialization(boolean isFinished) throws Exception {
        HybridSourceSplit split = createSerializedSplit(isFinished);
        HybridSourceSplitSerializer serializer = new HybridSourceSplitSerializer();
        byte[] serialized = serializer.serialize(split);
        HybridSourceSplit clonedSplit = serializer.deserialize(1, serialized);
        assertThat(clonedSplit).isEqualTo(split);
        assertThat(clonedSplit.isFinished).isEqualTo(isFinished);

        assertThatThrownBy(() -> serializer.deserialize(2, serialized))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testBackwardCompatibility() throws Exception {
        HybridSourceSplit split = createSerializedSplit(true);
        byte[] serializedV0 = deserializeV0(split);

        HybridSourceSplitSerializer serializer = new HybridSourceSplitSerializer();
        HybridSourceSplit clonedSplit = serializer.deserialize(0, serializedV0);

        // version 0 doesn't serialize the isFinished field, so it should get the default false.
        HybridSourceSplit expectedSplit = createSerializedSplit(false);
        assertThat(clonedSplit).isEqualTo(expectedSplit);
    }

    private byte[] deserializeV0(HybridSourceSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(split.sourceIndex());
            out.writeUTF(split.splitId());
            out.writeInt(split.wrappedSplitSerializerVersion());
            out.writeInt(split.wrappedSplitBytes().length);
            out.write(split.wrappedSplitBytes());
            out.writeBoolean(split.isFinished);
            out.flush();
            return baos.toByteArray();
        }
    }

    private HybridSourceSplit createSerializedSplit(boolean isFinished) {
        Map<Integer, Source> switchedSources = new HashMap<>();
        switchedSources.put(0, new MockSource(null, 0));
        byte[] splitBytes = {1, 2, 3};
        HybridSourceSplit split = new HybridSourceSplit(0, splitBytes, 0, "splitId", isFinished);
        return split;
    }
}
