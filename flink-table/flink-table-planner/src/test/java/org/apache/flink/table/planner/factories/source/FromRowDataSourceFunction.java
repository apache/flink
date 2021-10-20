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

package org.apache.flink.table.planner.factories.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * A {@link SourceFunction} which collects specific static {@link RowData} without serialization.
 */
public class FromRowDataSourceFunction implements SourceFunction<RowData> {
    private final String dataId;
    private volatile boolean isRunning = true;

    public FromRowDataSourceFunction(String dataId) {
        this.dataId = dataId;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        Collection<RowData> values =
                TestValuesTableFactory.getRegisteredRowData()
                        .getOrDefault(dataId, Collections.emptyList());
        Iterator<RowData> valueIter = values.iterator();

        while (isRunning && valueIter.hasNext()) {
            ctx.collect(valueIter.next());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
