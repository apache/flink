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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.utils.print.PrintStyle;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.List;
import java.util.function.Function;

/** Create result provider from a static set of data using external types. */
@Internal
class StaticResultProvider implements ResultProvider {

    /**
     * This converter supports only String, long, int and boolean fields. Moreover, this converter
     * works only with {@link GenericRowData}.
     */
    static final RowDataToStringConverter SIMPLE_ROW_DATA_TO_STRING_CONVERTER =
            rowData -> {
                GenericRowData genericRowData = (GenericRowData) rowData;
                String[] results = new String[rowData.getArity()];
                for (int i = 0; i < results.length; i++) {
                    Object value = genericRowData.getField(i);
                    if (Boolean.TRUE.equals(value)) {
                        results[i] = "TRUE";
                    } else if (Boolean.FALSE.equals(value)) {
                        results[i] = "FALSE";
                    } else {
                        results[i] = value == null ? PrintStyle.NULL_VALUE : "" + value;
                    }
                }
                return results;
            };

    private final List<Row> rows;
    private final Function<Row, RowData> externalToInternalConverter;

    public StaticResultProvider(List<Row> rows) {
        this(rows, StaticResultProvider::rowToInternalRow);
    }

    public StaticResultProvider(
            List<Row> rows, Function<Row, RowData> externalToInternalConverter) {
        this.rows = rows;
        this.externalToInternalConverter = externalToInternalConverter;
    }

    @Override
    public StaticResultProvider setJobClient(JobClient jobClient) {
        return this;
    }

    @Override
    public CloseableIterator<RowData> toInternalIterator() {
        return CloseableIterator.adapterForIterator(
                this.rows.stream().map(this.externalToInternalConverter).iterator());
    }

    @Override
    public CloseableIterator<Row> toExternalIterator() {
        return CloseableIterator.adapterForIterator(this.rows.iterator());
    }

    @Override
    public RowDataToStringConverter getRowDataStringConverter() {
        return SIMPLE_ROW_DATA_TO_STRING_CONVERTER;
    }

    @Override
    public boolean isFirstRowReady() {
        return true;
    }

    /** This function supports only String, long, int and boolean fields. */
    @VisibleForTesting
    static RowData rowToInternalRow(Row row) {
        Object[] values = new Object[row.getArity()];
        for (int i = 0; i < row.getArity(); i++) {
            Object value = row.getField(i);
            if (value == null) {
                values[i] = null;
            } else if (value instanceof String) {
                values[i] = StringData.fromString((String) value);
            } else if (value instanceof Boolean
                    || value instanceof Long
                    || value instanceof Integer) {
                values[i] = value;
            } else {
                throw new TableException("Cannot convert row type");
            }
        }

        return GenericRowData.of(values);
    }
}
