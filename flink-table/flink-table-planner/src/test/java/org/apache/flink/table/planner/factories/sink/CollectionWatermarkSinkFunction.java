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

package org.apache.flink.table.planner.factories.sink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesRuntimeHelper;

import java.util.LinkedList;

/** A sink function to collect the watermark. */
public class CollectionWatermarkSinkFunction extends RetractingSinkFunction {
    private final String tableName;

    public CollectionWatermarkSinkFunction(String tableName, DataStructureConverter converter) {
        super(tableName, converter);
        this.tableName = tableName;
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        TestValuesRuntimeHelper.getWatermarkHistory()
                .computeIfAbsent(tableName, k -> new LinkedList<>())
                .add(new Watermark(context.currentWatermark()));
        super.invoke(value, context);
    }
}
