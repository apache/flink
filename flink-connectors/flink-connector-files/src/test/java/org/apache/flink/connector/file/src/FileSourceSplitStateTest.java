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

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Unit tests for the {@link FileSourceSplitState}. */
public class FileSourceSplitStateTest {

    @Test
    public void testRoundTripWithoutModification() {
        final FileSourceSplit split = getTestSplit();
        final FileSourceSplitState state = new FileSourceSplitState(split);

        final FileSourceSplit resultSplit = state.toFileSourceSplit();

        assertEquals(split.getReaderPosition(), resultSplit.getReaderPosition());
    }

    @Test
    public void testStateStartsWithSplitValues() {
        final FileSourceSplit split = getTestSplit(new CheckpointedPosition(123L, 456L));
        final FileSourceSplitState state = new FileSourceSplitState(split);

        assertEquals(123L, state.getOffset());
        assertEquals(456L, state.getRecordsToSkipAfterOffset());
    }

    @Test
    public void testNewSplitTakesModifiedOffsetAndCount() {
        final FileSourceSplit split = getTestSplit();
        final FileSourceSplitState state = new FileSourceSplitState(split);

        state.setPosition(1234L, 7566L);
        final Optional<CheckpointedPosition> position =
                state.toFileSourceSplit().getReaderPosition();

        assertTrue(position.isPresent());
        assertEquals(new CheckpointedPosition(1234L, 7566L), position.get());
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
                new String[] {"localhost"},
                position);
    }
}
