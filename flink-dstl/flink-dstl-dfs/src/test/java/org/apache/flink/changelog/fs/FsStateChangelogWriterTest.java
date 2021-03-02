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

package org.apache.flink.changelog.fs;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import static org.apache.flink.shaded.guava18.com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/** {@link FsStateChangelogWriter} test. */
public class FsStateChangelogWriterTest {
    private static final int KEY_GROUP = 0;
    private final Random random = new Random();

    @Test
    public void testAppend() throws IOException {
        withWriter(
                (writer, store) -> {
                    writer.append(KEY_GROUP, getBytes());
                    writer.append(KEY_GROUP, getBytes());
                    writer.append(KEY_GROUP, getBytes());
                    assertNoUpload(store, "append shouldn't persist");
                });
    }

    @Test
    public void testPreUpload() throws IOException {
        int threshold = 1000;
        withWriter(
                threshold,
                (writer, store) -> {
                    byte[] bytes = getBytes(threshold);
                    SequenceNumber sqn = append(writer, bytes);
                    assertSubmittedOnly(store, bytes);
                    store.reset();
                    writer.persist(sqn);
                    assertNoUpload(store, "changes should have been pre-uploaded");
                    writer.persist(sqn);
                    assertSubmittedOnly(store, bytes); // 2nd persist should re-upload
                });
    }

    @Test
    public void testPersist() throws IOException {
        withWriter(
                (writer, store) -> {
                    byte[] bytes = getBytes();
                    writer.persist(append(writer, bytes));
                    assertSubmittedOnly(store, bytes);
                });
    }

    @Test
    public void testPersistAgain() throws IOException {
        withWriter(
                (writer, store) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn = append(writer, bytes);
                    writer.persist(sqn);
                    store.completeAndReset();
                    writer.confirm(sqn, writer.lastAppendedSqnUnsafe().next());
                    writer.persist(sqn);
                    assertNoUpload(store, "confirmed changes shouldn't be re-uploaded");
                });
    }

    @Test
    public void testPersistAgainBeforeCompletion() throws IOException {
        withWriter(
                (writer, store) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn = append(writer, bytes);
                    writer.persist(sqn);
                    store.reset();
                    writer.persist(sqn);
                    assertSubmittedOnly(store, bytes);
                });
    }

    @Test
    public void testPersistNewlyAppended() throws IOException {
        withWriter(
                (writer, store) -> {
                    SequenceNumber sqn = append(writer, getBytes());
                    writer.persist(sqn);
                    store.completeAndReset();
                    byte[] bytes = getBytes();
                    sqn = append(writer, bytes);
                    writer.persist(sqn);
                    assertSubmittedOnly(store, bytes);
                });
    }

    /** Emulates checkpoint abortion followed by a new checkpoint. */
    @Test
    public void testPersistAfterReset() throws IOException {
        withWriter(
                (writer, store) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn = append(writer, bytes);
                    writer.reset(sqn, SequenceNumber.of(Long.MAX_VALUE));
                    store.reset();
                    writer.persist(sqn);
                    assertSubmittedOnly(store, bytes);
                });
    }

    @Test
    public void testPersistAfterFailure() throws IOException {
        withWriter(
                (writer, store) -> {
                    byte[] bytes = getBytes();
                    SequenceNumber sqn = append(writer, bytes);
                    store.failUpload();
                    store.reset();
                    writer.persist(sqn);
                    assertSubmittedOnly(store, bytes);
                });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTruncate() throws IOException {
        withWriter(
                (writer, store) -> {
                    SequenceNumber sqn = append(writer, getBytes());
                    writer.truncate(sqn.next());
                    writer.persist(sqn);
                });
    }

    private void withWriter(
            BiConsumerWithException<FsStateChangelogWriter, TestingStateChangeUploader, IOException>
                    test)
            throws IOException {
        withWriter(1000, test);
    }

    private void withWriter(
            int appendPersistThreshold,
            BiConsumerWithException<FsStateChangelogWriter, TestingStateChangeUploader, IOException>
                    test)
            throws IOException {
        TestingStateChangeUploader store = new TestingStateChangeUploader();
        try (FsStateChangelogWriter writer =
                new FsStateChangelogWriter(
                        UUID.randomUUID(),
                        KeyGroupRange.of(KEY_GROUP, KEY_GROUP),
                        store,
                        appendPersistThreshold)) {
            test.accept(writer, store);
        }
    }

    private void assertSubmittedOnly(TestingStateChangeUploader store, byte[] bytes) {
        assertArrayEquals(
                bytes, getOnlyElement(getOnlyElement(store.getSaved()).getChanges()).getChange());
    }

    private SequenceNumber append(FsStateChangelogWriter writer, byte[] bytes) throws IOException {
        SequenceNumber sqn = writer.lastAppendedSequenceNumber().next();
        writer.append(KEY_GROUP, bytes);
        return sqn;
    }

    private byte[] getBytes() {
        return getBytes(10);
    }

    private byte[] getBytes(int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    private static void assertNoUpload(TestingStateChangeUploader store, String message) {
        assertTrue(message, store.getSaved().isEmpty());
    }
}
