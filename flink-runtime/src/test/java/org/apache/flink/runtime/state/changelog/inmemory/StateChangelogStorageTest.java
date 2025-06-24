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
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleReader;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link InMemoryStateChangelogStorage} test. */
public class StateChangelogStorageTest<T extends ChangelogStateHandle> {

    private final Random random = new Random();

    @TempDir public File temporaryFolder;

    public static Stream<Boolean> parameters() {
        return Stream.of(true);
    }

    @Disabled("FLINK-30729")
    @MethodSource("parameters")
    @ParameterizedTest(name = "compression = {0}")
    public void testNoAppendAfterClose(boolean compression) throws IOException {
        assertThatThrownBy(
                        () -> {
                            StateChangelogWriter<?> writer =
                                    getFactory(compression, temporaryFolder)
                                            .createWriter(
                                                    new OperatorID().toString(),
                                                    KeyGroupRange.of(0, 0),
                                                    new SyncMailboxExecutor());
                            writer.close();
                            writer.append(0, new byte[0]);
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    @MethodSource("parameters")
    @ParameterizedTest(name = "compression = {0}")
    public void testWriteAndRead(boolean compression) throws Exception {
        KeyGroupRange kgRange = KeyGroupRange.of(0, 5);
        Map<Integer, List<byte[]>> appendsByKeyGroup = generateAppends(kgRange, 405, 20);

        try (StateChangelogStorage<T> client = getFactory(compression, temporaryFolder);
                StateChangelogWriter<T> writer =
                        client.createWriter(
                                new OperatorID().toString(), kgRange, new SyncMailboxExecutor())) {
            SequenceNumber prev = writer.initialSequenceNumber();
            for (Map.Entry<Integer, List<byte[]>> entry : appendsByKeyGroup.entrySet()) {
                Integer group = entry.getKey();
                List<byte[]> appends = entry.getValue();
                for (byte[] bytes : appends) {
                    writer.append(group, bytes);
                }
                writer.nextSequenceNumber();
            }

            SnapshotResult<T> res = writer.persist(prev, 1).get();
            T jmHandle = res.getJobManagerOwnedSnapshot();
            StateChangelogHandleReader<T> reader = client.createReader();
            assertByteMapsEqual(appendsByKeyGroup, extract(jmHandle, reader));
        }
    }

    private void assertByteMapsEqual(
            Map<Integer, List<byte[]>> expected, Map<Integer, List<byte[]>> actual) {
        assertThat(actual).hasSameSizeAs(expected);
        for (Map.Entry<Integer, List<byte[]>> e : expected.entrySet()) {
            List<byte[]> expectedList = e.getValue();
            List<byte[]> actualList = actual.get(e.getKey());
            Iterator<byte[]> ite = expectedList.iterator(), ale = actualList.iterator();
            while (ite.hasNext() && ale.hasNext()) {
                assertThat(ale.next()).isEqualTo(ite.next());
            }
            assertThat(ite).isExhausted();
            assertThat(ale).isExhausted();
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

    protected StateChangelogStorage<T> getFactory(boolean compression, File temporaryFolder)
            throws IOException {
        return (StateChangelogStorage<T>) new InMemoryStateChangelogStorage();
    }
}
