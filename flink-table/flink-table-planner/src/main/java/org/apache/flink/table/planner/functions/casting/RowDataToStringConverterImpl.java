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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.utils.CastExecutor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.table.utils.print.PrintStyle;
import org.apache.flink.table.utils.print.RowDataToStringConverter;

import java.time.ZoneId;
import java.util.List;
import java.util.function.Function;

/** {@link RowData} to {@link String} converter using {@link CastRule}. */
@Internal
public final class RowDataToStringConverterImpl implements RowDataToStringConverter {

    private final DataType dataType;
    private final CastRule.Context castRuleContext;

    private Function<RowData, String>[] columnConverters;

    @VisibleForTesting
    public RowDataToStringConverterImpl(DataType dataType) {
        this(
                dataType,
                DateTimeUtils.UTC_ZONE.toZoneId(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    public RowDataToStringConverterImpl(
            DataType dataType, ZoneId zoneId, ClassLoader classLoader, boolean legacyBehaviour) {
        this.dataType = dataType;
        this.castRuleContext = CastRule.Context.create(true, legacyBehaviour, zoneId, classLoader);
    }

    @SuppressWarnings("unchecked")
    private void init() {
        List<DataType> rowDataTypes = DataType.getFieldDataTypes(dataType);
        this.columnConverters = new Function[rowDataTypes.size()];

        for (int i = 0; i < rowDataTypes.size(); i++) {
            final int index = i;
            LogicalType fieldType = rowDataTypes.get(index).getLogicalType();
            RowData.FieldGetter getter = RowData.createFieldGetter(fieldType, index);
            CastExecutor<Object, StringData> castExecutor =
                    (CastExecutor<Object, StringData>)
                            CastRuleProvider.create(
                                    castRuleContext, fieldType, VarCharType.STRING_TYPE);
            if (castExecutor == null) {
                throw new IllegalStateException(
                        "Cannot create a cast executor for converting "
                                + fieldType
                                + " to string. This is a bug, please open an issue.");
            }
            this.columnConverters[index] =
                    row -> {
                        if (row.isNullAt(index)) {
                            return PrintStyle.NULL_VALUE;
                        }
                        return castExecutor.cast(getter.getFieldOrNull(row)).toString();
                    };
        }
    }

    @Override
    public String[] convert(RowData rowData) {
        if (this.columnConverters == null) {
            init();
        }

        String[] result = new String[rowData.getArity()];
        for (int i = 0; i < result.length; i++) {
            result[i] = this.columnConverters[i].apply(rowData);
        }
        return result;
    }
}
