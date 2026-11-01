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

package org.apache.flink.connector.file.src;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.file.src.enumerate.GlobFileEnumerator;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Deterministic discovery/checkpoint tests with glob inputs and real local files. */
class FileSourceGlobEnumeratorTest {

    @TempDir private java.nio.file.Path directory;

    @Test
    void testRestoreRetainsPendingFilesAndDoesNotRediscoverProcessedFiles() throws Exception {
        final FileSource<String> source =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(),
                                new Path(directory.resolve("part-*/*.txt").toUri()))
                        .setFileEnumerator(
                                () -> new GlobFileEnumerator(new NonSplittingRecursiveEnumerator()))
                        .monitorContinuously(Duration.ofSeconds(1))
                        .build();
        final Path first = write("part-1/first.txt");
        final TestingSplitEnumeratorContext<FileSourceSplit> context =
                new TestingSplitEnumeratorContext<>(1);
        final PendingSplitsCheckpoint<FileSourceSplit> checkpoint;
        final Path pending;
        try (SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>> enumerator =
                source.createEnumerator(context)) {
            enumerator.start();
            context.registerReader(0, "localhost");
            enumerator.addReader(0);
            enumerator.handleSplitRequest(0, "localhost");
            assertThat(context.getSplitAssignments().get(0).getAssignedSplits())
                    .extracting(FileSourceSplit::path)
                    .containsExactly(first);

            pending = write("part-2/pending.txt");
            context.triggerAllActions();
            checkpoint = enumerator.snapshotState(1);
            assertThat(checkpoint.getSplits())
                    .extracting(FileSourceSplit::path)
                    .containsExactly(pending);
            assertThat(checkpoint.getAlreadyProcessedPaths())
                    .containsExactlyInAnyOrder(first, pending);
        }

        final SimpleVersionedSerializer<PendingSplitsCheckpoint<FileSourceSplit>> serializer =
                source.getEnumeratorCheckpointSerializer();
        final PendingSplitsCheckpoint<FileSourceSplit> restoredCheckpoint =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(checkpoint));
        final TestingSplitEnumeratorContext<FileSourceSplit> restoredContext =
                new TestingSplitEnumeratorContext<>(1);
        try (SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>> restored =
                source.restoreEnumerator(restoredContext, restoredCheckpoint)) {
            restored.start();
            final Path newFile = write("part-3/new.txt");
            restoredContext.triggerAllActions();
            restoredContext.triggerAllActions();
            assertThat(restored.snapshotState(2).getSplits())
                    .extracting(FileSourceSplit::path)
                    .containsExactlyInAnyOrder(pending, newFile);
            assertThat(restored.snapshotState(2).getAlreadyProcessedPaths())
                    .containsExactlyInAnyOrder(first, pending, newFile);
        }
    }

    private Path write(String relative) throws Exception {
        final java.nio.file.Path file = directory.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.writeString(file, relative);
        return new Path(file.toUri());
    }
}
