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

package org.apache.flink.connector.file.table.stream.compact;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.EndCheckpoint;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorInput;
import static org.apache.flink.connector.file.table.stream.compact.CompactMessages.InputFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompactFileWriter}. */
class CompactFileWriterTest extends AbstractCompactTestBase {

    @Test
    void testEmitEndCheckpointAfterEndInput() throws Exception {
        CompactFileWriter<RowData> compactFileWriter =
                new CompactFileWriter<>(
                        1000, StreamingFileSink.forRowFormat(folder, new SimpleStringEncoder<>()));
        try (OneInputStreamOperatorTestHarness<RowData, CoordinatorInput> harness =
                new OneInputStreamOperatorTestHarness<>(compactFileWriter)) {
            harness.setup();
            harness.open();

            harness.processElement(row("test"), 0);
            harness.snapshot(1, 1);
            harness.notifyOfCompletedCheckpoint(1);

            List<CoordinatorInput> coordinatorInputs = harness.extractOutputValues();

            assertThat(coordinatorInputs).hasSize(2);
            // assert emit InputFile
            assertThat(coordinatorInputs.get(0)).isInstanceOf(InputFile.class);
            // assert emit EndCheckpoint
            assertThat(((EndCheckpoint) coordinatorInputs.get(1)).getCheckpointId()).isEqualTo(1);

            harness.processElement(row("test1"), 0);
            harness.processElement(row("test2"), 0);

            harness.getOutput().clear();

            // end input
            harness.endInput();
            coordinatorInputs = harness.extractOutputValues();
            // assert emit EndCheckpoint with Long.MAX_VALUE lastly
            EndCheckpoint endCheckpoint =
                    (EndCheckpoint) coordinatorInputs.get(coordinatorInputs.size() - 1);
            assertThat(endCheckpoint.getCheckpointId()).isEqualTo(Long.MAX_VALUE);
        }
    }

    private static RowData row(String s) {
        return GenericRowData.of(StringData.fromString(s));
    }
}
