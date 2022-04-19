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

package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.api.java.typeutils.runtime.EitherSerializer;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.FileSinkCommittableSerializer;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy.Builder;
import org.apache.flink.connector.file.sink.compactor.operator.CompactCoordinator;
import org.apache.flink.connector.file.sink.compactor.operator.CompactCoordinatorStateHandler;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorRequest;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorRequestSerializer;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils.TestInProgressFileRecoverable;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils.TestPendingFileRecoverable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Either;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/** Test for {@link CompactCoordinator}. */
public class CompactCoordinatorTest extends AbstractCompactTestBase {

    @Test
    public void testSizeThreshold() throws Exception {
        FileCompactStrategy strategy = Builder.newBuilder().setSizeThreshold(10).build();
        CompactCoordinator coordinator =
                new CompactCoordinator(strategy, getTestCommittableSerializer());

        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<FileSinkCommittable>, CompactorRequest>
                harness = new OneInputStreamOperatorTestHarness<>(coordinator)) {
            harness.setup();
            harness.open();

            FileSinkCommittable committable0 = committable("0", ".0", 5);
            FileSinkCommittable committable1 = committable("0", ".1", 6);
            harness.processElement(message(committable0));
            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.processElement(message(committable1));

            List<CompactorRequest> results = harness.extractOutputValues();
            Assert.assertEquals(1, results.size());
            assertToCompact(results.get(0), committable0, committable1);

            harness.processElement(message(committable("0", ".2", 5)));
            harness.processElement(message(committable("1", ".0", 5)));

            Assert.assertEquals(1, harness.extractOutputValues().size());
        }
    }

    @Test
    public void testCompactOnCheckpoint() throws Exception {
        FileCompactStrategy strategy = Builder.newBuilder().enableCompactionOnCheckpoint(1).build();
        CompactCoordinator coordinator =
                new CompactCoordinator(strategy, getTestCommittableSerializer());

        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<FileSinkCommittable>, CompactorRequest>
                harness = new OneInputStreamOperatorTestHarness<>(coordinator)) {
            harness.setup();
            harness.open();

            FileSinkCommittable passThroughCommittable = committable("0", "4", 5);
            FileSinkCommittable committable0 = committable("0", ".0", 5);
            FileSinkCommittable committable1 = committable("0", ".1", 6);
            FileSinkCommittable committable2 = committable("0", ".2", 5);
            FileSinkCommittable committable3 = committable("1", ".0", 5);

            harness.processElement(message(passThroughCommittable));
            harness.processElement(message(committable0));
            harness.processElement(message(committable1));

            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.prepareSnapshotPreBarrier(1);
            harness.snapshot(1, 1);

            Assert.assertEquals(1, harness.extractOutputValues().size());

            harness.processElement(message(committable2));
            harness.processElement(message(committable3));

            Assert.assertEquals(1, harness.extractOutputValues().size());

            harness.prepareSnapshotPreBarrier(2);
            harness.snapshot(2, 2);

            List<CompactorRequest> results = harness.extractOutputValues();
            Assert.assertEquals(3, results.size());
            assertToCompact(results.get(0), committable0, committable1);
            assertToPassthrough(results.get(0), passThroughCommittable);
            assertToCompact(results.get(1), committable2);
            assertToCompact(results.get(2), committable3);
        }
    }

    @Test
    public void testCompactOverMultipleCheckpoints() throws Exception {
        FileCompactStrategy strategy = Builder.newBuilder().enableCompactionOnCheckpoint(3).build();
        CompactCoordinator coordinator =
                new CompactCoordinator(strategy, getTestCommittableSerializer());

        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<FileSinkCommittable>, CompactorRequest>
                harness = new OneInputStreamOperatorTestHarness<>(coordinator)) {
            harness.setup();
            harness.open();

            FileSinkCommittable committable0 = committable("0", ".0", 5);
            FileSinkCommittable committable1 = committable("0", ".1", 6);

            harness.processElement(message(committable0));
            harness.processElement(message(committable1));

            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.prepareSnapshotPreBarrier(1);
            harness.snapshot(1, 1);
            harness.prepareSnapshotPreBarrier(2);
            harness.snapshot(2, 2);

            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.prepareSnapshotPreBarrier(3);
            harness.snapshot(3, 3);

            List<CompactorRequest> results = harness.extractOutputValues();
            Assert.assertEquals(1, results.size());
            assertToCompact(results.get(0), committable0, committable1);
        }
    }

    @Test
    public void testCompactOnEndOfInput() throws Exception {
        FileCompactStrategy strategy = Builder.newBuilder().setSizeThreshold(10).build();
        CompactCoordinator coordinator =
                new CompactCoordinator(strategy, getTestCommittableSerializer());

        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<FileSinkCommittable>, CompactorRequest>
                harness = new OneInputStreamOperatorTestHarness<>(coordinator)) {
            harness.setup();
            harness.open();

            FileSinkCommittable committable0 = committable("0", ".0", 5);

            harness.processElement(message(committable0));

            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.prepareSnapshotPreBarrier(1);
            harness.snapshot(1, 1);

            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.endInput();

            List<CompactorRequest> results = harness.extractOutputValues();
            Assert.assertEquals(1, results.size());
            assertToCompact(results.get(0), committable0);
        }
    }

    @Test
    public void testPassthrough() throws Exception {
        FileCompactStrategy strategy = Builder.newBuilder().setSizeThreshold(10).build();
        CompactCoordinator coordinator =
                new CompactCoordinator(strategy, getTestCommittableSerializer());

        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<FileSinkCommittable>, CompactorRequest>
                harness = new OneInputStreamOperatorTestHarness<>(coordinator)) {
            harness.setup();
            harness.open();

            FileSinkCommittable cleanupToPassthrough = cleanupInprogress("0", ".0", 1);
            FileSinkCommittable sizeUnavailableToPassthrough = committable("0", ".1", -1);
            FileSinkCommittable pathNotHidToPassThrough = committable("0", "2", -1);
            FileSinkCommittable normalCommittable = committable("0", ".3", 10);

            harness.processElement(message(cleanupToPassthrough));
            harness.processElement(message(sizeUnavailableToPassthrough));
            harness.processElement(message(pathNotHidToPassThrough));
            harness.processElement(message(normalCommittable));

            List<CompactorRequest> results = harness.extractOutputValues();
            Assert.assertEquals(1, results.size());
            assertToCompact(results.get(0), normalCommittable);
            assertToPassthrough(
                    results.get(0),
                    cleanupToPassthrough,
                    sizeUnavailableToPassthrough,
                    pathNotHidToPassThrough);
        }
    }

    @Test
    public void testRestore() throws Exception {
        FileCompactStrategy strategy = Builder.newBuilder().setSizeThreshold(10).build();
        CompactCoordinator coordinator =
                new CompactCoordinator(strategy, getTestCommittableSerializer());

        FileSinkCommittable committable0 = committable("0", ".0", 5);
        FileSinkCommittable committable1 = committable("0", ".1", 6);
        FileSinkCommittable committable2 = committable("0", ".2", 5);
        FileSinkCommittable committable3 = committable("1", ".0", 5);

        OperatorSubtaskState state;
        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<FileSinkCommittable>, CompactorRequest>
                harness = new OneInputStreamOperatorTestHarness<>(coordinator)) {
            harness.setup();
            harness.open();

            harness.processElement(message(committable0));
            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.prepareSnapshotPreBarrier(1);
            state = harness.snapshot(1, 1);
        }

        coordinator = new CompactCoordinator(strategy, getTestCommittableSerializer());
        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<FileSinkCommittable>, CompactorRequest>
                harness = new OneInputStreamOperatorTestHarness<>(coordinator)) {
            harness.setup();
            harness.initializeState(state);
            harness.open();

            harness.processElement(message(committable1));

            Assert.assertEquals(1, harness.extractOutputValues().size());

            harness.processElement(message(committable2));
            harness.processElement(message(committable3));

            Assert.assertEquals(1, harness.extractOutputValues().size());

            harness.endInput();

            List<CompactorRequest> results = harness.extractOutputValues();
            Assert.assertEquals(3, results.size());
            assertToCompact(results.get(0), committable0, committable1);
            assertToCompact(results.get(1), committable2);
            assertToCompact(results.get(2), committable3);
        }
    }

    @Test
    public void testRestoreWithChangedStrategy() throws Exception {
        FileCompactStrategy strategy = Builder.newBuilder().setSizeThreshold(100).build();
        CompactCoordinator coordinator =
                new CompactCoordinator(strategy, getTestCommittableSerializer());

        FileSinkCommittable committable0 = committable("0", ".0", 5);
        FileSinkCommittable committable1 = committable("0", ".1", 6);
        FileSinkCommittable committable2 = committable("0", ".2", 7);
        FileSinkCommittable committable3 = committable("0", ".3", 8);
        FileSinkCommittable committable4 = committable("0", ".4", 9);
        FileSinkCommittable committable5 = committable("0", ".5", 2);

        OperatorSubtaskState state;
        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<FileSinkCommittable>, CompactorRequest>
                harness = new OneInputStreamOperatorTestHarness<>(coordinator)) {
            harness.setup();
            harness.open();

            harness.processElement(message(committable0));
            harness.processElement(message(committable1));
            harness.processElement(message(committable2));
            harness.processElement(message(committable3));
            harness.processElement(message(committable4));

            harness.prepareSnapshotPreBarrier(1);
            state = harness.snapshot(1, 1);

            Assert.assertEquals(0, harness.extractOutputValues().size());
        }

        FileCompactStrategy changedStrategy = Builder.newBuilder().setSizeThreshold(10).build();
        CompactCoordinator changedCoordinator =
                new CompactCoordinator(changedStrategy, getTestCommittableSerializer());
        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<FileSinkCommittable>, CompactorRequest>
                harness = new OneInputStreamOperatorTestHarness<>(changedCoordinator)) {
            harness.setup();
            harness.initializeState(state);
            harness.open();

            Assert.assertEquals(2, harness.extractOutputValues().size());

            harness.processElement(message(committable5));

            List<CompactorRequest> results = harness.extractOutputValues();
            Assert.assertEquals(3, results.size());
            assertToCompact(results.get(0), committable0, committable1);
            assertToCompact(results.get(1), committable2, committable3);
            assertToCompact(results.get(2), committable4, committable5);
        }
    }

    @Test
    public void testStateHandler() throws Exception {
        FileCompactStrategy strategy = Builder.newBuilder().setSizeThreshold(10).build();
        CompactCoordinator coordinator =
                new CompactCoordinator(strategy, getTestCommittableSerializer());

        // with . prefix
        FileSinkCommittable committable0 = committable("0", ".0", 5);
        FileSinkCommittable committable1 = committable("0", ".1", 6);

        // without . prefix
        FileSinkCommittable committable2 = committable("0", "2", 6);

        FileSinkCommittable cleanup3 = cleanupInprogress("0", "3", 7);

        OperatorSubtaskState state;
        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<FileSinkCommittable>, CompactorRequest>
                harness = new OneInputStreamOperatorTestHarness<>(coordinator)) {
            harness.setup();
            harness.open();

            harness.processElement(message(committable0));
            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.processElement(message(cleanup3));
            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.prepareSnapshotPreBarrier(1);
            state = harness.snapshot(1, 1);
        }

        CompactCoordinatorStateHandler handler =
                new CompactCoordinatorStateHandler(getTestCommittableSerializer());
        try (OneInputStreamOperatorTestHarness<
                        CommittableMessage<FileSinkCommittable>,
                        Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>>
                harness = new OneInputStreamOperatorTestHarness<>(handler)) {
            harness.setup(
                    new EitherSerializer<>(
                            new SimpleVersionedSerializerTypeSerializerProxy<>(
                                    () ->
                                            new CommittableMessageSerializer<>(
                                                    getTestCommittableSerializer())),
                            new SimpleVersionedSerializerTypeSerializerProxy<>(
                                    () ->
                                            new CompactorRequestSerializer(
                                                    getTestCommittableSerializer()))));
            harness.initializeState(state);
            harness.open();

            Assert.assertEquals(2, harness.extractOutputValues().size());

            harness.processElement(message(committable1));
            harness.processElement(message(committable2));

            List<Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>> results =
                    harness.extractOutputValues();
            Assert.assertEquals(4, results.size());

            // restored request
            Assert.assertTrue(results.get(0).isRight());
            assertToCompact(results.get(0).right(), committable0);

            assertToPassthrough(results.get(1).right(), cleanup3);

            // committable with . prefix should also be passed through
            Assert.assertTrue(
                    results.get(2).isLeft()
                            && results.get(2).left() instanceof CommittableWithLineage);
            Assert.assertEquals(
                    ((CommittableWithLineage<FileSinkCommittable>) results.get(2).left())
                            .getCommittable(),
                    committable1);

            // committable without . prefix should be passed through normally
            Assert.assertTrue(
                    results.get(3).isLeft()
                            && results.get(3).left() instanceof CommittableWithLineage);
            Assert.assertEquals(
                    ((CommittableWithLineage<FileSinkCommittable>) results.get(3).left())
                            .getCommittable(),
                    committable2);
        }
    }

    private StreamRecord<CommittableMessage<FileSinkCommittable>> message(
            FileSinkCommittable committable) {
        return new StreamRecord<>(new CommittableWithLineage<>(committable, 1L, 0), 0L);
    }

    private FileSinkCommittable committable(String bucketId, String name, int size)
            throws IOException {
        // put bucketId after name to keep the possible '.' prefix in name
        return new FileSinkCommittable(
                bucketId,
                new TestPendingFileRecoverable(
                        newFile(name + "_" + bucketId, size <= 0 ? 1 : size), size));
    }

    private FileSinkCommittable cleanupInprogress(String bucketId, String name, int size)
            throws IOException {
        Path toCleanup = newFile(name + "_" + bucketId, size);
        return new FileSinkCommittable(
                bucketId, new TestInProgressFileRecoverable(toCleanup, size));
    }

    private SimpleVersionedSerializer<FileSinkCommittable> getTestCommittableSerializer() {
        return new FileSinkCommittableSerializer(
                new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                        FileSinkTestUtils.TestPendingFileRecoverable::new),
                new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                        FileSinkTestUtils.TestInProgressFileRecoverable::new));
    }

    private void assertToCompact(CompactorRequest request, FileSinkCommittable... committables) {
        List<FileSinkCommittable> committableToCompact = request.getCommittableToCompact();
        Assert.assertArrayEquals(committables, committableToCompact.toArray());
    }

    private void assertToPassthrough(
            CompactorRequest request, FileSinkCommittable... committables) {
        List<FileSinkCommittable> committableToCompact = request.getCommittableToPassthrough();
        Assert.assertArrayEquals(committables, committableToCompact.toArray());
    }
}
