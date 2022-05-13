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

package org.apache.flink.table.runtime.operators.python.utils;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/** The collector is used to convert a {@link RowData} to a {@link StreamRecord}. */
public class StreamRecordRowDataWrappingCollector implements Collector<RowData> {

    private final Collector<StreamRecord<RowData>> out;

    /** For Table API & SQL jobs, the timestamp field is not used. */
    private final StreamRecord<RowData> reuseStreamRecord = new StreamRecord<>(null);

    public StreamRecordRowDataWrappingCollector(Collector<StreamRecord<RowData>> out) {
        this.out = out;
    }

    @Override
    public void collect(RowData record) {
        out.collect(reuseStreamRecord.replace(record));
    }

    @Override
    public void close() {
        out.close();
    }
}
