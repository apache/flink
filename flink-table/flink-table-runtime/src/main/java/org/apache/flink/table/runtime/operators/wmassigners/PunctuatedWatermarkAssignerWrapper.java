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
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

/** Generates periodic watermarks based on a {@link PunctuatedWatermarkAssigner}. */
@Internal
public class PunctuatedWatermarkAssignerWrapper
        implements AssignerWithPunctuatedWatermarks<RowData> {
    private static final long serialVersionUID = 1L;
    private final PunctuatedWatermarkAssigner assigner;
    private final int timeFieldIdx;
    private final DataFormatConverters.DataFormatConverter<RowData, Row> converter;

    /**
     * @param timeFieldIdx the index of the rowtime attribute.
     * @param assigner the watermark assigner.
     * @param sourceType the type of source
     */
    @SuppressWarnings("unchecked")
    public PunctuatedWatermarkAssignerWrapper(
            PunctuatedWatermarkAssigner assigner, int timeFieldIdx, DataType sourceType) {
        this.assigner = assigner;
        this.timeFieldIdx = timeFieldIdx;
        DataType originDataType;
        if (sourceType instanceof FieldsDataType) {
            originDataType = sourceType;
        } else {
            originDataType = DataTypes.ROW(DataTypes.FIELD("f0", sourceType));
        }
        converter =
                DataFormatConverters.getConverterForDataType(originDataType.bridgedTo(Row.class));
    }

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(RowData row, long extractedTimestamp) {
        long timestamp = row.getLong(timeFieldIdx);
        return assigner.getWatermark(converter.toExternal(row), timestamp);
    }

    @Override
    public long extractTimestamp(RowData element, long recordTimestamp) {
        return 0;
    }
}
