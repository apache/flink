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

package org.apache.flink.python.util;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.types.Row;

/**
 * {@link PartitionCustomTestMapFunction} is a dedicated MapFunction to make sure the specific field
 * data is equal to current sub-task index.
 */
public class PartitionCustomTestMapFunction extends RichMapFunction<Row, Row> {

    private int currentTaskIndex;

    @Override
    public void open(OpenContext openContext) {
        this.currentTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public Row map(Row value) throws Exception {
        int expectedPartitionIndex =
                (Integer) (value.getField(1)) % getRuntimeContext().getNumberOfParallelSubtasks();
        if (expectedPartitionIndex != currentTaskIndex) {
            throw new RuntimeException(
                    String.format(
                            "the data: Row<%s> was sent to the wrong partition[%d], "
                                    + "expected partition is [%d].",
                            value.toString(), currentTaskIndex, expectedPartitionIndex));
        }
        return value;
    }
}
