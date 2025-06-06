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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;

import java.util.List;
import java.util.stream.IntStream;

/** Forwards partition keys of the given input's row. */
@Internal
public class PassPartitionKeysCollector extends PassThroughCollectorBase {

    private final ProjectedRowData[] partitionKeys;

    public PassPartitionKeysCollector(
            Output<StreamRecord<RowData>> output,
            ChangelogMode changelogMode,
            List<RuntimeTableSemantics> tableSemantics) {
        super(output, changelogMode, tableSemantics.size());
        partitionKeys = new ProjectedRowData[tableSemantics.size()];
        IntStream.range(0, tableSemantics.size())
                .forEach(
                        pos ->
                                partitionKeys[pos] =
                                        ProjectedRowData.from(
                                                tableSemantics.get(pos).partitionByColumns()));
    }

    @Override
    public void setPrefix(int pos, RowData input) {
        prefix = partitionKeys[pos].replaceRow(input);
    }
}
