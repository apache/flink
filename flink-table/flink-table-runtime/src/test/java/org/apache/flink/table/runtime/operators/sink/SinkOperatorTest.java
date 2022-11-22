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

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOutputFormatOperatorFactory;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link SinkOperator}. */
public class SinkOperatorTest {

    @Test
    public void testSinkOperatorWithOutputFormat() {
        SinkOperator operator =
                new SinkOperator(
                        new OutputFormatSinkFunction<>(
                                new OutputFormat<RowData>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void configure(Configuration parameters) {}

                                    @Override
                                    public void open(int taskNumber, int numTasks) {}

                                    @Override
                                    public void writeRecord(RowData record) {}

                                    @Override
                                    public void close() {}
                                }),
                        -1);
        SimpleOperatorFactory<?> factory = SimpleOperatorFactory.of(operator);
        assertThat(factory).isInstanceOf(SimpleOutputFormatOperatorFactory.class);
    }
}
