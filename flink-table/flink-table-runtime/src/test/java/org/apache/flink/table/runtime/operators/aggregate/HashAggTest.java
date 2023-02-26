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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test hash agg. */
public class HashAggTest {

    private static final int MEMORY_SIZE = 1024 * 1024 * 32;

    private Map<Integer, Long> outputMap = new HashMap<>();
    private MemoryManager memoryManager =
            MemoryManagerBuilder.newBuilder().setMemorySize(MEMORY_SIZE).build();
    private IOManager ioManager;
    private SumHashAggTestOperator operator;

    @Before
    public void before() throws Exception {
        ioManager = new IOManagerAsync();
        operator =
                new SumHashAggTestOperator(40 * 32 * 1024) {

                    @Override
                    Object getOwner() {
                        return HashAggTest.this;
                    }

                    @Override
                    MemoryManager getMemoryManager() {
                        return memoryManager;
                    }

                    @Override
                    Collector<StreamRecord<RowData>> getOutput() {
                        return new Collector<StreamRecord<RowData>>() {
                            @Override
                            public void collect(StreamRecord<RowData> record) {
                                RowData row = record.getValue();
                                outputMap.put(
                                        row.isNullAt(0) ? null : row.getInt(0),
                                        row.isNullAt(1) ? null : row.getLong(1));
                            }

                            @Override
                            public void close() {}
                        };
                    }

                    @Override
                    public IOManager getIOManager() {
                        return ioManager;
                    }
                };

        operator.open();
    }

    @After
    public void afterTest() throws Exception {
        this.ioManager.close();

        if (this.memoryManager != null) {
            assertThat(this.memoryManager.verifyEmpty())
                    .as("Memory leak: not all segments have been returned to the memory manager.")
                    .isTrue();
            this.memoryManager.shutdown();
            this.memoryManager = null;
        }
    }

    private void addRow(RowData row) throws Exception {
        operator.processElement(new StreamRecord<>(row));
    }

    @Test
    public void testNormal() throws Exception {
        addRow(GenericRowData.of(1, 1L));
        addRow(GenericRowData.of(5, 2L));
        addRow(GenericRowData.of(2, 3L));
        addRow(GenericRowData.of(2, null));
        addRow(GenericRowData.of(1, 4L));
        addRow(GenericRowData.of(4, 5L));
        addRow(GenericRowData.of(1, 6L));
        addRow(GenericRowData.of(1, null));
        addRow(GenericRowData.of(2, 8L));
        addRow(GenericRowData.of(5, 9L));
        addRow(GenericRowData.of(10, null));
        addRow(GenericRowData.of(null, 5L));

        operator.endInput();
        operator.close();

        Map<Integer, Long> expected = new HashMap<>();
        expected.put(null, 5L);
        expected.put(1, 11L);
        expected.put(2, 11L);
        expected.put(4, 5L);
        expected.put(5, 11L);
        expected.put(10, null);
        assertThat(outputMap).isEqualTo(expected);
    }

    @Test
    public void testSpill() throws Exception {
        for (int i = 0; i < 30000; i++) {
            addRow(GenericRowData.of(i, (long) i));
            addRow(GenericRowData.of(i + 1, (long) i));
        }
        addRow(GenericRowData.of(1, null));
        addRow(GenericRowData.of(null, 5L));
        operator.endInput();
        operator.close();
        assertThat(outputMap).hasSize(30002);
    }
}
