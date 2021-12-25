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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.utils.CastExecutor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.table.utils.print.PrintStyle;
import org.apache.flink.table.utils.print.RowDataToStringConverter;

import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.flink.table.api.DataTypes.STRING;

/** {@link RowData} to {@link String} converter using {@link CastRule}. */
public final class RowDataToStringConverterImpl implements RowDataToStringConverter {

    private final Function<RowData, String>[] columnConverters;

    @VisibleForTesting
    public RowDataToStringConverterImpl(DataType dataType) {
        this(
                dataType,
                DateTimeUtils.UTC_ZONE.toZoneId(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    @SuppressWarnings("unchecked")
    public RowDataToStringConverterImpl(
            DataType dataType, ZoneId zoneId, ClassLoader classLoader, boolean legacyBehaviour) {
        List<DataType> rowDataTypes = DataType.getFieldDataTypes(dataType);
        this.columnConverters = new Function[rowDataTypes.size()];

        for (int i = 0; i < rowDataTypes.size(); i++) {
            final int index = i;
            LogicalType fieldType = rowDataTypes.get(index).getLogicalType();
            RowData.FieldGetter getter = RowData.createFieldGetter(fieldType, index);
            CastExecutor<Object, StringData> castExecutor =
                    (CastExecutor<Object, StringData>)
                            CastRuleProvider.create(
                                    CastRule.Context.create(legacyBehaviour, zoneId, classLoader),
                                    fieldType,
                                    STRING().getLogicalType());
            if (castExecutor == null) {
                // Fallback in case no casting rule is defined, for example for MULTISET and
                // STRUCTURED
                // Links to https://issues.apache.org/jira/browse/FLINK-24403
                this.columnConverters[index] =
                        row -> {
                            if (row.isNullAt(index)) {
                                return PrintStyle.NULL_VALUE;
                            }
                            return Objects.toString(getter.getFieldOrNull(row));
                        };
            } else {
                this.columnConverters[index] =
                        row -> {
                            if (row.isNullAt(index)) {
                                return PrintStyle.NULL_VALUE;
                            }
                            return castExecutor.cast(getter.getFieldOrNull(row)).toString();
                        };
            }
        }
    }

    @Override
    public String[] convert(RowData rowData) {
        String[] result = new String[rowData.getArity()];
        for (int i = 0; i < result.length; i++) {
            result[i] = this.columnConverters[i].apply(rowData);
        }
        return result;
    }
}
