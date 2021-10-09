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

package org.apache.flink.runtime.state.changelog.inmemory;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleReader;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.util.CloseableIterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** {@link InMemoryStateChangelogStorage} test. */
public class StateChangelogStorageTest<T extends ChangelogStateHandle> {

    private final Random random = new Random();

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test(expected = IllegalStateException.class)
    public void testNoAppendAfterClose() throws IOException {
        StateChangelogWriter<?> writer =
                getFactory().createWriter(new OperatorID().toString(), KeyGroupRange.of(0, 0));
        writer.close();
        writer.append(0, new byte[0]);
    }

    @Test
    public void testWriteAndRead() throws Exception {
        KeyGroupRange kgRange = KeyGroupRange.of(0, 5);
        Map<Integer, List<byte[]>> appendsByKeyGroup = generateAppends(kgRange, 10, 20);

        try (StateChangelogStorage<T> client = getFactory();
                StateChangelogWriter<T> writer =
                        client.createWriter(new OperatorID().toString(), kgRange)) {
            SequenceNumber prev = writer.initialSequenceNumber();
            for (Map.Entry<Integer, List<byte[]>> entry : appendsByKeyGroup.entrySet()) {
                Integer group = entry.getKey();
                List<byte[]> appends = entry.getValue();
                for (byte[] bytes : appends) {
                    writer.append(group, bytes);
                }
            }

            T handle = writer.persist(prev).get();
            StateChangelogHandleReader<T> reader = client.createReader();

            assertByteMapsEqual(appendsByKeyGroup, extract(handle, reader));
        }
    }

    private void assertByteMapsEqual(
            Map<Integer, List<byte[]>> expected, Map<Integer, List<byte[]>> actual) {
        assertEquals(expected.size(), actual.size());
        for (Map.Entry<Integer, List<byte[]>> e : expected.entrySet()) {
            List<byte[]> expectedList = e.getValue();
            List<byte[]> actualList = actual.get(e.getKey());
            Iterator<byte[]> ite = expectedList.iterator(), ale = actualList.iterator();
            while (ite.hasNext() && ale.hasNext()) {
                assertArrayEquals(ite.next(), ale.next());
            }
            assertFalse(ite.hasNext());
            assertFalse(ale.hasNext());
        }
    }

    private Map<Integer, List<byte[]>> extract(T handle, StateChangelogHandleReader<T> reader)
            throws Exception {
        Map<Integer, List<byte[]>> changes = new HashMap<>();
        try (CloseableIterator<StateChange> it = reader.getChanges(handle)) {
            while (it.hasNext()) {
                StateChange change = it.next();
                changes.computeIfAbsent(change.getKeyGroup(), k -> new ArrayList<>())
                        .add(change.getChange());
            }
        }
        return changes;
    }

    private Map<Integer, List<byte[]>> generateAppends(
            KeyGroupRange kgRange, int keyLen, int appendsPerGroup) {
        return stream(kgRange.spliterator(), false)
                .collect(toMap(identity(), unused -> generateData(appendsPerGroup, keyLen)));
    }

    private List<byte[]> generateData(int numAppends, int keyLen) {
        return Stream.generate(() -> randomBytes(keyLen))
                .limit(numAppends)
                .collect(Collectors.toList());
    }

    private byte[] randomBytes(int len) {
        byte[] bytes = new byte[len];
        random.nextBytes(bytes);
        return bytes;
    }

    protected StateChangelogStorage<T> getFactory() throws IOException {
        return (StateChangelogStorage<T>) new InMemoryStateChangelogStorage();
    }
}
