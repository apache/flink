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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

/**
 * Operator that assigns the current watermark as the timestamp to each incoming StreamRecord.
 *
 * <p>This is used in conjunction with {@link WatermarkCompactingSinkMaterializer} which buffers
 * records by their timestamp. Without meaningful timestamps, all records would be buffered under
 * the same key, breaking the watermark-based compaction logic.
 *
 * <p>If the current watermark is {@code Long.MIN_VALUE} (the initial state before any watermark
 * arrives), records will be assigned that value and will be compacted when the first watermark
 * arrives.
 */
@Internal
public class WatermarkTimestampAssigner extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        element.setTimestamp(currentWatermark);
        output.collect(element);
    }
}
