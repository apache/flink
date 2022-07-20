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

package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.wmstrategies.PeriodicWatermarkAssigner;

import javax.annotation.Nullable;

/** Generates periodic watermarks based on a {@link PeriodicWatermarkAssigner}. */
@Internal
public class PeriodicWatermarkAssignerWrapper implements AssignerWithPeriodicWatermarks<RowData> {
    private static final long serialVersionUID = 1L;
    private final PeriodicWatermarkAssigner assigner;
    private final int timeFieldIdx;

    /**
     * @param timeFieldIdx the index of the rowtime attribute.
     * @param assigner the watermark assigner.
     */
    public PeriodicWatermarkAssignerWrapper(PeriodicWatermarkAssigner assigner, int timeFieldIdx) {
        this.assigner = assigner;
        this.timeFieldIdx = timeFieldIdx;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return assigner.getWatermark();
    }

    @Override
    public long extractTimestamp(RowData row, long recordTimestamp) {
        long timestamp = row.getTimestamp(timeFieldIdx, 3).getMillisecond();
        assigner.nextTimestamp(timestamp);
        return 0;
    }
}
