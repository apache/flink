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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.checkpoint.CheckpointOptions.AlignmentType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.apache.flink.runtime.checkpoint.CheckpointOptions.NO_ALIGNED_CHECKPOINT_TIME_OUT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.FULL_CHECKPOINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Tests for the {@link CheckpointOptions} class. */
class CheckpointOptionsTest {

    @Test
    void testDefaultCheckpoint() throws Exception {
        final CheckpointOptions options = CheckpointOptions.forCheckpointWithDefaultLocation();
        assertThat(options.getCheckpointType()).isEqualTo(CheckpointType.CHECKPOINT);
        assertThat(options.getTargetLocation().isDefaultReference()).isTrue();

        final CheckpointOptions copy = CommonTestUtils.createCopySerializable(options);
        assertThat(copy.getCheckpointType()).isEqualTo(CheckpointType.CHECKPOINT);
        assertThat(copy.getTargetLocation().isDefaultReference()).isTrue();
    }

    @Test
    void testSavepoint() throws Exception {
        final Random rnd = new Random();
        final byte[] locationBytes = new byte[rnd.nextInt(41) + 1];
        rnd.nextBytes(locationBytes);

        final SnapshotType[] snapshotTypes = {
            CHECKPOINT,
            FULL_CHECKPOINT,
            SavepointType.savepoint(SavepointFormatType.CANONICAL),
            SavepointType.suspend(SavepointFormatType.CANONICAL),
            SavepointType.terminate(SavepointFormatType.CANONICAL)
        };

        final CheckpointOptions options =
                new CheckpointOptions(
                        snapshotTypes[rnd.nextInt(snapshotTypes.length)],
                        new CheckpointStorageLocationReference(locationBytes));

        final CheckpointOptions copy = CommonTestUtils.createCopySerializable(options);
        assertThat(copy.getCheckpointType()).isEqualTo(options.getCheckpointType());
        assertThat(copy.getTargetLocation().getReferenceBytes()).isEqualTo(locationBytes);
    }

    @Test
    void testSavepointNeedsAlignment() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                new CheckpointOptions(
                                        SavepointType.savepoint(SavepointFormatType.CANONICAL),
                                        CheckpointStorageLocationReference.getDefault(),
                                        AlignmentType.UNALIGNED,
                                        0));
        ;
    }

    @Test
    void testCheckpointNeedsAlignment() {
        CheckpointStorageLocationReference location =
                CheckpointStorageLocationReference.getDefault();
        assertThat(
                        new CheckpointOptions(
                                        CHECKPOINT,
                                        location,
                                        AlignmentType.UNALIGNED,
                                        NO_ALIGNED_CHECKPOINT_TIME_OUT)
                                .needsAlignment())
                .isFalse();
        assertThat(
                        new CheckpointOptions(
                                        CHECKPOINT,
                                        location,
                                        AlignmentType.ALIGNED,
                                        NO_ALIGNED_CHECKPOINT_TIME_OUT)
                                .needsAlignment())
                .isTrue();
        assertThat(
                        new CheckpointOptions(
                                        CHECKPOINT,
                                        location,
                                        AlignmentType.FORCED_ALIGNED,
                                        NO_ALIGNED_CHECKPOINT_TIME_OUT)
                                .needsAlignment())
                .isTrue();
        assertThat(
                        new CheckpointOptions(
                                        CHECKPOINT,
                                        location,
                                        AlignmentType.AT_LEAST_ONCE,
                                        NO_ALIGNED_CHECKPOINT_TIME_OUT)
                                .needsAlignment())
                .isFalse();
    }

    @Test
    void testCheckpointIsTimeoutable() {
        CheckpointStorageLocationReference location =
                CheckpointStorageLocationReference.getDefault();
        assertTimeoutable(
                CheckpointOptions.alignedWithTimeout(CheckpointType.CHECKPOINT, location, 10),
                false,
                true,
                10);
        assertTimeoutable(
                CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location),
                true,
                false,
                NO_ALIGNED_CHECKPOINT_TIME_OUT);
        assertTimeoutable(
                CheckpointOptions.alignedWithTimeout(CheckpointType.CHECKPOINT, location, 10)
                        .withUnalignedUnsupported(),
                false,
                false,
                10);
        assertTimeoutable(
                CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location)
                        .withUnalignedUnsupported(),
                false,
                false,
                NO_ALIGNED_CHECKPOINT_TIME_OUT);
    }

    @Test
    void testForceAlignmentIsReversable() {
        CheckpointStorageLocationReference location =
                CheckpointStorageLocationReference.getDefault();
        assertReversable(
                CheckpointOptions.alignedWithTimeout(CheckpointType.CHECKPOINT, location, 10),
                true);
        assertReversable(CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, location), true);

        assertReversable(CheckpointOptions.alignedNoTimeout(CHECKPOINT, location), false);
        assertReversable(
                CheckpointOptions.alignedNoTimeout(
                        SavepointType.savepoint(SavepointFormatType.CANONICAL), location),
                false);
        assertReversable(CheckpointOptions.notExactlyOnce(CHECKPOINT, location), false);
        assertReversable(
                CheckpointOptions.notExactlyOnce(
                        SavepointType.savepoint(SavepointFormatType.CANONICAL), location),
                false);
    }

    private void assertReversable(CheckpointOptions options, boolean forceHasEffect) {
        assertThat(options.withUnalignedSupported())
                .as("all non-forced options support unaligned mode")
                .isEqualTo(options);
        CheckpointOptions unalignedUnsupported = options.withUnalignedUnsupported();
        if (forceHasEffect) {
            assertThat(unalignedUnsupported)
                    .as("expected changes in the options")
                    .isNotEqualTo(options);
        } else {
            assertThat(unalignedUnsupported)
                    .as("not expected changes to the options")
                    .isEqualTo(options);
        }
        assertThat(unalignedUnsupported.withUnalignedSupported())
                .as("expected fully reversable options")
                .isEqualTo(options);
    }

    private void assertTimeoutable(
            CheckpointOptions options, boolean isUnaligned, boolean isTimeoutable, long timeout) {
        assertThat(options.isExactlyOnceMode()).as("exactly once").isTrue();
        assertThat(options.needsAlignment()).as("need alignment").isEqualTo(!isUnaligned);
        assertThat(options.isUnalignedCheckpoint()).as("unaligned").isEqualTo(isUnaligned);
        assertThat(options.isTimeoutable()).as("timeoutable").isEqualTo(isTimeoutable);
        assertThat(options.getAlignedCheckpointTimeout()).as("timeout").isEqualTo(timeout);
    }
}
