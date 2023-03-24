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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowKindSetter}. */
public class RowKindSetterTest {

    @Test
    public void testSetRowKind() throws Exception {
        // test set to all row kind
        for (RowKind targetRowKind : RowKind.values()) {
            RowKindSetter rowKindSetter = new RowKindSetter(targetRowKind);
            try (OneInputStreamOperatorTestHarness<RowData, RowData> operatorTestHarness =
                    new OneInputStreamOperatorTestHarness<>(rowKindSetter)) {
                operatorTestHarness.open();

                // get the rows with all row kind
                List<RowData> rows = getRowsWithAllRowKind();
                for (RowData row : rows) {
                    operatorTestHarness.processElement(new StreamRecord<>(row));
                }
                // verify the row kind of output
                verifyRowKind(operatorTestHarness.extractOutputValues(), targetRowKind);
            }
        }
    }

    private List<RowData> getRowsWithAllRowKind() {
        List<RowData> rows = new ArrayList<>();
        for (RowKind rowKind : RowKind.values()) {
            rows.add(GenericRowData.of(rowKind, 1));
        }
        return rows;
    }

    private void verifyRowKind(List<RowData> rows, RowKind rowKind) {
        for (RowData row : rows) {
            assertThat(row.getRowKind()).isEqualTo(rowKind);
        }
    }
}
