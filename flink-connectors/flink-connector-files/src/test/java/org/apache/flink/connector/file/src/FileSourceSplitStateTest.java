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

import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link FileSourceSplitState}. */
class FileSourceSplitStateTest {

    @Test
    void testRoundTripWithoutModification() {
        final FileSourceSplit split = getTestSplit();
        final FileSourceSplitState state = new FileSourceSplitState(split);

        final FileSourceSplit resultSplit = state.toFileSourceSplit();

        assertThat(resultSplit.getReaderPosition()).isEqualTo(split.getReaderPosition());
    }

    @Test
    void testStateStartsWithSplitValues() {
        final FileSourceSplit split = getTestSplit(new CheckpointedPosition(123L, 456L));
        final FileSourceSplitState state = new FileSourceSplitState(split);

        assertThat(state.getOffset()).isEqualTo(123L);
        assertThat(state.getRecordsToSkipAfterOffset()).isEqualTo(456L);
    }

    @Test
    void testNewSplitTakesModifiedOffsetAndCount() {
        final FileSourceSplit split = getTestSplit();
        final FileSourceSplitState state = new FileSourceSplitState(split);

        state.setPosition(1234L, 7566L);
        final Optional<CheckpointedPosition> position =
                state.toFileSourceSplit().getReaderPosition();

        assertThat(position).isPresent();
        assertThat(position.get()).isEqualTo(new CheckpointedPosition(1234L, 7566L));
    }

    // ------------------------------------------------------------------------

    private static FileSourceSplit getTestSplit() {
        return getTestSplit(null);
    }

    private static FileSourceSplit getTestSplit(CheckpointedPosition position) {
        return new FileSourceSplit(
                "test-id",
                new Path("file:/some/random/path"),
                17,
                121,
                0,
                150,
                new String[] {"localhost"},
                position);
    }
}
