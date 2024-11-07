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

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.FileSinkCommittableSerializer;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorOperator;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorOperatorStateHandler;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorRequest;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils.TestInProgressFileRecoverable;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils.TestPendingFileRecoverable;
import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils.IntDecoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.InProgressFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedCompactingFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.WriterProperties;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Either;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions.committableSummary;
import static org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions.committableWithLineage;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompactorOperator}. */
class CompactorOperatorTest extends AbstractCompactTestBase {

    @Test
    void testCompact() throws Exception {
        FileCompactor fileCompactor =
                new RecordWiseFileCompactor<>(new DecoderBasedReader.Factory<>(IntDecoder::new));
        CompactorOperator compactor = createTestOperator(fileCompactor);

        try (OneInputStreamOperatorTestHarness<
                        CompactorRequest, CommittableMessage<FileSinkCommittable>>
                harness = new OneInputStreamOperatorTestHarness<>(compactor)) {
            harness.setup();
            harness.open();

            harness.processElement(
                    request(
                            "0",
                            Arrays.asList(committable("0", ".0", 5), committable("0", ".1", 5)),
                            null));

            assertThat(harness.extractOutputValues()).isEmpty();

            harness.prepareSnapshotPreBarrier(1);
            harness.snapshot(1, 1L);
            harness.notifyOfCompletedCheckpoint(1);

            compactor.getAllTasksFuture().join();

            assertThat(harness.extractOutputValues()).isEmpty();

            harness.prepareSnapshotPreBarrier(2);

            // 1summary+1compacted+2cleanup
            ListAssert<CommittableMessage<FileSinkCommittable>> results =
                    assertThat(harness.extractOutputValues()).hasSize(4);
            results.element(1, as(committableWithLineage()))
                    .hasCommittable(committable("0", "compacted-0", 10));
            results.element(2, as(committableWithLineage())).hasCommittable(cleanupPath("0", ".0"));
            results.element(3, as(committableWithLineage())).hasCommittable(cleanupPath("0", ".1"));
        }
    }

    @Test
    void testPassthrough() throws Exception {
        FileCompactor fileCompactor =
                new RecordWiseFileCompactor<>(new DecoderBasedReader.Factory<>(IntDecoder::new));
        CompactorOperator compactor = createTestOperator(fileCompactor);

        try (OneInputStreamOperatorTestHarness<
                        CompactorRequest, CommittableMessage<FileSinkCommittable>>
                harness = new OneInputStreamOperatorTestHarness<>(compactor)) {
            harness.setup();
            harness.open();

            FileSinkCommittable cleanupInprogressRequest = cleanupInprogress("0", "0", 1);
            FileSinkCommittable cleanupPathRequest = cleanupPath("0", "1");

            harness.processElement(
                    request("0", null, Collections.singletonList(cleanupInprogressRequest)));
            harness.processElement(
                    request("0", null, Collections.singletonList(cleanupPathRequest)));

            assertThat(harness.extractOutputValues()).isEmpty();

            harness.prepareSnapshotPreBarrier(1);
            harness.snapshot(1, 1L);
            harness.notifyOfCompletedCheckpoint(1);

            compactor.getAllTasksFuture().join();

            assertThat(harness.extractOutputValues()).isEmpty();

            harness.prepareSnapshotPreBarrier(2);

            ListAssert<CommittableMessage<FileSinkCommittable>> messages =
                    assertThat(harness.extractOutputValues()).hasSize(3);
            messages.element(1, as(committableWithLineage()))
                    .hasCommittable(cleanupInprogressRequest);
            messages.element(2, as(committableWithLineage())).hasCommittable(cleanupPathRequest);
        }
    }

    @Test
    void testRestore() throws Exception {
        FileCompactor fileCompactor =
                new RecordWiseFileCompactor<>(new DecoderBasedReader.Factory<>(IntDecoder::new));
        CompactorOperator compactor = createTestOperator(fileCompactor);

        OperatorSubtaskState state;
        try (OneInputStreamOperatorTestHarness<
                        CompactorRequest, CommittableMessage<FileSinkCommittable>>
                harness = new OneInputStreamOperatorTestHarness<>(compactor)) {
            harness.setup();
            harness.open();

            harness.processElement(
                    request(
                            "0",
                            Arrays.asList(committable("0", ".0", 5), committable("0", ".1", 5)),
                            null));
            harness.snapshot(1, 1L);

            harness.processElement(
                    request(
                            "0",
                            Arrays.asList(committable("0", ".2", 5), committable("0", ".3", 5)),
                            null));

            harness.notifyOfCompletedCheckpoint(1);

            // request 1 is submitted and request 2 is pending
            state = harness.snapshot(2, 2L);
        }

        compactor = createTestOperator(fileCompactor);
        try (OneInputStreamOperatorTestHarness<
                        CompactorRequest, CommittableMessage<FileSinkCommittable>>
                harness = new OneInputStreamOperatorTestHarness<>(compactor)) {
            harness.setup();
            harness.initializeState(state);
            harness.open();

            // request 1 should be submitted
            compactor.getAllTasksFuture().join();
            harness.prepareSnapshotPreBarrier(3);

            // the result of request 1 should be emitted
            assertThat(harness.extractOutputValues()).hasSize(4);

            harness.snapshot(3, 3L);
            harness.notifyOfCompletedCheckpoint(3L);

            // request 2 should be submitted
            compactor.getAllTasksFuture().join();
            harness.prepareSnapshotPreBarrier(4);

            // the result of request 2 should be emitted
            assertThat(harness.extractOutputValues()).hasSize(8);

            // 1summary+1compacted+2cleanup * 2
            ListAssert<CommittableMessage<FileSinkCommittable>> results =
                    assertThat(harness.extractOutputValues()).hasSize(8);
            results.element(0, as(committableSummary()));
            results.element(1, as(committableWithLineage()))
                    .hasCommittable(committable("0", "compacted-0", 10));
            results.element(2, as(committableWithLineage())).hasCommittable(cleanupPath("0", ".0"));
            results.element(3, as(committableWithLineage())).hasCommittable(cleanupPath("0", ".1"));

            results.element(4, as(committableSummary()));
            results.element(5, as(committableWithLineage()))
                    .hasCommittable(committable("0", "compacted-2", 10));
            results.element(6, as(committableWithLineage())).hasCommittable(cleanupPath("0", ".2"));
            results.element(7, as(committableWithLineage())).hasCommittable(cleanupPath("0", ".3"));
        }
    }

    @Test
    void testStateHandler() throws Exception {
        FileCompactor fileCompactor =
                new RecordWiseFileCompactor<>(new DecoderBasedReader.Factory<>(IntDecoder::new));
        CompactorOperator compactor = createTestOperator(fileCompactor);

        OperatorSubtaskState state;
        try (OneInputStreamOperatorTestHarness<
                        CompactorRequest, CommittableMessage<FileSinkCommittable>>
                harness = new OneInputStreamOperatorTestHarness<>(compactor)) {
            harness.setup();
            harness.open();

            harness.processElement(
                    request(
                            "0",
                            Arrays.asList(committable("0", ".0", 1), committable("0", ".1", 2)),
                            null));
            harness.snapshot(1, 1L);

            harness.processElement(
                    request(
                            "0",
                            Arrays.asList(committable("0", ".2", 3), committable("0", ".3", 4)),
                            null));

            harness.notifyOfCompletedCheckpoint(1);

            // request 1 is submitted and request 2 is pending
            state = harness.snapshot(2, 2L);
        }

        CompactorOperatorStateHandler handler =
                new CompactorOperatorStateHandler(
                        null, getTestCommittableSerializer(), createTestBucketWriter());
        try (OneInputStreamOperatorTestHarness<
                        Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>,
                        CommittableMessage<FileSinkCommittable>>
                harness = new OneInputStreamOperatorTestHarness<>(handler)) {
            harness.setup();
            harness.initializeState(state);
            harness.open();

            // remaining requests from coordinator
            harness.processElement(
                    new StreamRecord<>(
                            Either.Right(
                                    request(
                                                    "0",
                                                    Collections.singletonList(
                                                            committable("0", ".4", 5)),
                                                    null)
                                            .getValue())));
            harness.processElement(
                    new StreamRecord<>(
                            Either.Right(
                                    request(
                                                    "0",
                                                    Collections.singletonList(
                                                            committable("0", ".5", 6)),
                                                    null)
                                            .getValue())));

            harness.processElement(
                    new StreamRecord<>(Either.Left(new CommittableSummary<>(0, 1, 3L, 2, 2, 0))));

            // remaining in-progress file from file writer
            harness.processElement(
                    new StreamRecord<>(
                            Either.Left(
                                    new CommittableWithLineage<>(
                                            committable("0", ".6", 7), 3L, 0))));

            // new pending file written this time
            harness.processElement(
                    new StreamRecord<>(
                            Either.Left(
                                    new CommittableWithLineage<>(
                                            committable("0", "7", 8), 3L, 0))));

            harness.processElement(
                    new StreamRecord<>(Either.Left(new CommittableSummary<>(0, 1, 4L, 0, 0, 0))));

            harness.processElement(
                    new StreamRecord<>(Either.Left(new CommittableSummary<>(0, 1, 5L, 3, 3, 0))));

            // 1 summary + (1 compacted committable + 1 compacted cleanup) * 6 + 1 hidden + 1 normal
            // + 1 summary + 1 cleanup + 1 summary
            ListAssert<CommittableMessage<FileSinkCommittable>> results =
                    assertThat(harness.extractOutputValues()).hasSize(18);
            results.element(0, as(committableSummary()));

            List<FileSinkCommittable> expectedResult =
                    Arrays.asList(
                            committable("0", "compacted-0", 1),
                            cleanupPath("0", ".0"),
                            committable("0", "compacted-1", 2),
                            cleanupPath("0", ".1"),
                            committable("0", "compacted-2", 3),
                            cleanupPath("0", ".2"),
                            committable("0", "compacted-3", 4),
                            cleanupPath("0", ".3"),
                            committable("0", "compacted-4", 5),
                            cleanupPath("0", ".4"),
                            committable("0", "compacted-5", 6),
                            cleanupPath("0", ".5"),
                            committable("0", "compacted-6", 7),
                            committable("0", "7", 8));

            for (int i = 0; i < expectedResult.size(); ++i) {
                results.element(i + 1, as(committableWithLineage()))
                        .hasCommittable(expectedResult.get(i));
            }

            results.element(15, as(committableSummary()));
            results.element(16, as(committableWithLineage()))
                    .hasCommittable(cleanupPath("0", ".6"));

            results.element(17, as(committableSummary()));
        }
    }

    @Test
    void testStateHandlerRestore() throws Exception {
        OperatorSubtaskState state;
        try (OneInputStreamOperatorTestHarness<
                        Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>,
                        CommittableMessage<FileSinkCommittable>>
                harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new CompactorOperatorStateHandler(
                                        null,
                                        getTestCommittableSerializer(),
                                        createTestBucketWriter()))) {
            harness.setup();
            harness.open();

            // remaining request from coordinator
            harness.processElement(
                    new StreamRecord<>(
                            Either.Right(
                                    request(
                                                    "0",
                                                    Collections.singletonList(
                                                            committable("0", ".1", 1)),
                                                    null)
                                            .getValue())));

            // process only summary during cp1, unaligned barrier may be processed ahead of the
            // elements
            harness.processElement(
                    new StreamRecord<>(Either.Left(new CommittableSummary<>(0, 1, 1L, 2, 2, 0))));

            state = harness.snapshot(1, 1L);

            ListAssert<CommittableMessage<FileSinkCommittable>> results =
                    assertThat(harness.extractOutputValues()).hasSize(3);
            results.element(0, as(committableSummary()));
            results.element(1, as(committableWithLineage()))
                    .hasCommittable(committable("0", "compacted-1", 1));
            results.element(2, as(committableWithLineage())).hasCommittable(cleanupPath("0", ".1"));
        }

        try (OneInputStreamOperatorTestHarness<
                        Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>,
                        CommittableMessage<FileSinkCommittable>>
                harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new CompactorOperatorStateHandler(
                                        null,
                                        getTestCommittableSerializer(),
                                        createTestBucketWriter()))) {
            harness.setup();
            harness.initializeState(state);
            harness.open();

            harness.processElement(
                    new StreamRecord<>(
                            Either.Left(
                                    new CommittableWithLineage<>(
                                            committable("0", ".2", 2), 1L, 0))));

            harness.processElement(
                    new StreamRecord<>(
                            Either.Left(
                                    new CommittableWithLineage<>(
                                            committable("0", "3", 3), 1L, 0))));

            state = harness.snapshot(2, 2L);

            ListAssert<CommittableMessage<FileSinkCommittable>> results =
                    assertThat(harness.extractOutputValues()).hasSize(2);
            results.element(0, as(committableWithLineage()))
                    .hasCommittable(committable("0", "2", 2));
            results.element(1, as(committableWithLineage()))
                    .hasCommittable(committable("0", "3", 3));
        }

        try (OneInputStreamOperatorTestHarness<
                        Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>,
                        CommittableMessage<FileSinkCommittable>>
                harness =
                        new OneInputStreamOperatorTestHarness<>(
                                new CompactorOperatorStateHandler(
                                        null,
                                        getTestCommittableSerializer(),
                                        createTestBucketWriter()))) {
            harness.setup();
            harness.initializeState(state);
            harness.open();

            harness.processElement(
                    new StreamRecord<>(Either.Left(new CommittableSummary<>(0, 1, 2L, 0, 0, 0))));

            ListAssert<CommittableMessage<FileSinkCommittable>> results =
                    assertThat(harness.extractOutputValues()).hasSize(2);
            results.element(0, as(committableSummary()));
            results.element(1, as(committableWithLineage())).hasCommittable(cleanupPath("0", ".2"));
        }
    }

    private StreamRecord<CompactorRequest> request(
            String bucketId,
            List<FileSinkCommittable> toCompact,
            List<FileSinkCommittable> toPassthrough) {
        return new StreamRecord<>(
                new CompactorRequest(
                        bucketId,
                        toCompact == null ? new ArrayList<>() : toCompact,
                        toPassthrough == null ? new ArrayList<>() : toPassthrough),
                0L);
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

    private FileSinkCommittable cleanupPath(String bucketId, String name) throws IOException {
        Path toCleanup = newFile(name + "_" + bucketId, 1);
        return new FileSinkCommittable(bucketId, toCleanup);
    }

    private SimpleVersionedSerializer<FileSinkCommittable> getTestCommittableSerializer() {
        return new FileSinkCommittableSerializer(
                new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                        TestPendingFileRecoverable::new),
                new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                        TestInProgressFileRecoverable::new));
    }

    private CompactorOperator createTestOperator(FileCompactor compactor) {
        return new CompactorOperator(
                null,
                FileCompactStrategy.Builder.newBuilder()
                        .setNumCompactThreads(2)
                        .enableCompactionOnCheckpoint(1)
                        .build(),
                getTestCommittableSerializer(),
                compactor,
                createTestBucketWriter());
    }

    private BucketWriter<?, String> createTestBucketWriter() {
        return new BucketWriter<Integer, String>() {

            @Override
            public InProgressFileWriter<Integer, String> openNewInProgressFile(
                    String bucketId, Path path, long creationTime) throws IOException {
                return new InProgressFileWriter<Integer, String>() {
                    BufferedWriter writer;
                    long size = 0L;

                    @Override
                    public void write(Integer element, long currentTime) throws IOException {
                        if (writer == null) {
                            writer = new BufferedWriter(new FileWriter(path.toString()));
                        }
                        writer.write(element);
                        size += 1;
                    }

                    @Override
                    public InProgressFileRecoverable persist() throws IOException {
                        return new TestInProgressFileRecoverable(path, size);
                    }

                    @Override
                    public PendingFileRecoverable closeForCommit() throws IOException {
                        return new TestPendingFileRecoverable(path, size);
                    }

                    @Override
                    public void dispose() {}

                    @Override
                    public String getBucketId() {
                        return bucketId;
                    }

                    @Override
                    public long getCreationTime() {
                        return 0;
                    }

                    @Override
                    public long getSize() throws IOException {
                        return size;
                    }

                    @Override
                    public long getLastUpdateTime() {
                        return 0;
                    }
                };
            }

            @Override
            public InProgressFileWriter<Integer, String> resumeInProgressFileFrom(
                    String s, InProgressFileRecoverable inProgressFileSnapshot, long creationTime)
                    throws IOException {
                return null;
            }

            @Override
            public WriterProperties getProperties() {
                return null;
            }

            @Override
            public PendingFile recoverPendingFile(PendingFileRecoverable pendingFileRecoverable)
                    throws IOException {
                return new PendingFile() {
                    @Override
                    public void commit() throws IOException {
                        TestPendingFileRecoverable testRecoverable =
                                (TestPendingFileRecoverable) pendingFileRecoverable;
                        if (testRecoverable.getPath() != null) {
                            if (!testRecoverable
                                    .getPath()
                                    .equals(testRecoverable.getUncommittedPath())) {
                                testRecoverable
                                        .getPath()
                                        .getFileSystem()
                                        .rename(
                                                testRecoverable.getUncommittedPath(),
                                                testRecoverable.getPath());
                            }
                        }
                    }

                    @Override
                    public void commitAfterRecovery() throws IOException {
                        commit();
                    }
                };
            }

            @Override
            public boolean cleanupInProgressFileRecoverable(
                    InProgressFileRecoverable inProgressFileRecoverable) throws IOException {
                return false;
            }

            @Override
            public CompactingFileWriter openNewCompactingFile(
                    CompactingFileWriter.Type type, String bucketId, Path path, long creationTime)
                    throws IOException {
                if (type == CompactingFileWriter.Type.RECORD_WISE) {
                    return openNewInProgressFile(bucketId, path, creationTime);
                } else {
                    FileOutputStream fileOutputStream = new FileOutputStream(path.toString());
                    return new OutputStreamBasedCompactingFileWriter() {

                        @Override
                        public OutputStream asOutputStream() throws IOException {
                            return fileOutputStream;
                        }

                        @Override
                        public PendingFileRecoverable closeForCommit() throws IOException {
                            fileOutputStream.flush();
                            return new TestPendingFileRecoverable(
                                    path, fileOutputStream.getChannel().position());
                        }
                    };
                }
            }
        };
    }
}
