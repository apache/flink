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

package org.apache.flink.connector.base.source.utils;

import org.apache.flink.connector.base.source.reader.mocks.TestingSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/** Test for {@link SerdeUtils}. */
public class SerdeUtilsTest {

    private static final int READER0 = 0;
    private static final int READER1 = 1;

    @Test
    public void testSerdeSplitAssignments() throws IOException {
        final Map<Integer, Set<TestingSourceSplit>> splitAssignments = new HashMap<>();

        final HashSet<TestingSourceSplit> splitsForReader0 = new HashSet<>();
        splitsForReader0.add(new TestingSourceSplit("split-0"));
        splitsForReader0.add(new TestingSourceSplit("split-1"));
        splitsForReader0.add(new TestingSourceSplit("split-2"));

        final HashSet<TestingSourceSplit> splitsForReader1 = new HashSet<>();
        splitsForReader1.add(new TestingSourceSplit("split-3"));
        splitsForReader1.add(new TestingSourceSplit("split-4"));
        splitsForReader1.add(new TestingSourceSplit("split-5"));

        splitAssignments.put(READER0, splitsForReader0);
        splitAssignments.put(READER1, splitsForReader1);

        final byte[] serializedSplitAssignments =
                SerdeUtils.serializeSplitAssignments(
                        splitAssignments, new TestingSourceSplitSerializer());

        final Map<Integer, HashSet<TestingSourceSplit>> deseredSplitAssignments =
                SerdeUtils.deserializeSplitAssignments(
                        serializedSplitAssignments,
                        new TestingSourceSplitSerializer(),
                        HashSet::new);

        assertEquals(splitAssignments, deseredSplitAssignments);
    }

    private static class TestingSourceSplitSerializer
            implements SimpleVersionedSerializer<TestingSourceSplit> {

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(TestingSourceSplit split) throws IOException {
            return split.splitId().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public TestingSourceSplit deserialize(int version, byte[] serialized) throws IOException {
            return new TestingSourceSplit(new String(serialized, StandardCharsets.UTF_8));
        }
    }
}
