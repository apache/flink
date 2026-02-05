/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Test the {@link TimestampedFileInputSplit} for Continuous File Processing. */
@ExtendWith(TestLoggerExtension.class)
public class TimestampedFileInputSplitTest {

    @Test
    public void testSplitEquality() {

        TimestampedFileInputSplit richFirstSplit =
                new TimestampedFileInputSplit(10, 2, new Path("test"), 0, 100, null);

        TimestampedFileInputSplit richSecondSplit =
                new TimestampedFileInputSplit(10, 2, new Path("test"), 0, 100, null);
        assertEquals(richFirstSplit, richSecondSplit);

        TimestampedFileInputSplit richModSecondSplit =
                new TimestampedFileInputSplit(11, 2, new Path("test"), 0, 100, null);
        assertNotEquals(richSecondSplit, richModSecondSplit);

        TimestampedFileInputSplit richThirdSplit =
                new TimestampedFileInputSplit(10, 2, new Path("test/test1"), 0, 100, null);
        assertEquals(10, richThirdSplit.getModificationTime());
        assertNotEquals(richFirstSplit, richThirdSplit);

        TimestampedFileInputSplit richThirdSplitCopy =
                new TimestampedFileInputSplit(10, 2, new Path("test/test1"), 0, 100, null);
        assertThat(richThirdSplit).isEqualTo(richThirdSplitCopy);
    }

    @Test
    public void testSplitComparison() {
        TimestampedFileInputSplit richFirstSplit =
                new TimestampedFileInputSplit(0, 3, new Path("test/test1"), 0, 100, null);

        TimestampedFileInputSplit richSecondSplit =
                new TimestampedFileInputSplit(10, 2, new Path("test/test2"), 0, 100, null);

        TimestampedFileInputSplit richThirdSplit =
                new TimestampedFileInputSplit(10, 1, new Path("test/test2"), 0, 100, null);

        TimestampedFileInputSplit richForthSplit =
                new TimestampedFileInputSplit(11, 0, new Path("test/test3"), 0, 100, null);

        TimestampedFileInputSplit richFifthSplit =
                new TimestampedFileInputSplit(11, 1, new Path("test/test3"), 0, 100, null);

        // smaller mod time
        assertThat(richFirstSplit).isLessThan(richSecondSplit);

        // lexicographically on the path
        assertThat(richThirdSplit).isLessThan(richFifthSplit);

        // same mod time, same file so smaller split number first
        assertThat(richThirdSplit).isLessThan(richSecondSplit);

        // smaller modification time first
        assertThat(richThirdSplit).isLessThan(richForthSplit);
    }

    @Test
    public void testIllegalArgument() {
        assertThatThrownBy(
                        () -> {
                            new TimestampedFileInputSplit(
                                    -10,
                                    2,
                                    new Path("test"),
                                    0,
                                    100,
                                    null); // invalid modification time
                        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testPriorityQ() {
        TimestampedFileInputSplit richFirstSplit =
                new TimestampedFileInputSplit(0, 3, new Path("test/test1"), 0, 100, null);

        TimestampedFileInputSplit richSecondSplit =
                new TimestampedFileInputSplit(10, 2, new Path("test/test2"), 0, 100, null);

        TimestampedFileInputSplit richThirdSplit =
                new TimestampedFileInputSplit(10, 1, new Path("test/test2"), 0, 100, null);

        TimestampedFileInputSplit richForthSplit =
                new TimestampedFileInputSplit(11, 0, new Path("test/test3"), 0, 100, null);

        TimestampedFileInputSplit richFifthSplit =
                new TimestampedFileInputSplit(11, 1, new Path("test/test3"), 0, 100, null);

        Queue<TimestampedFileInputSplit> pendingSplits = new PriorityQueue<>();

        pendingSplits.add(richSecondSplit);
        pendingSplits.add(richForthSplit);
        pendingSplits.add(richFirstSplit);
        pendingSplits.add(richFifthSplit);
        pendingSplits.add(richFifthSplit);
        pendingSplits.add(richThirdSplit);

        List<TimestampedFileInputSplit> actualSortedSplits = new ArrayList<>();
        do {
            actualSortedSplits.add(pendingSplits.poll());
        } while (!pendingSplits.isEmpty());

        List<TimestampedFileInputSplit> expectedSortedSplits = new ArrayList<>();
        expectedSortedSplits.add(richFirstSplit);
        expectedSortedSplits.add(richThirdSplit);
        expectedSortedSplits.add(richSecondSplit);
        expectedSortedSplits.add(richForthSplit);
        expectedSortedSplits.add(richFifthSplit);
        expectedSortedSplits.add(richFifthSplit);

        assertThat(actualSortedSplits).containsExactlyElementsOf(expectedSortedSplits);
    }
}
