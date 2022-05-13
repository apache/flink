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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.Collections;

/** Internal interface with internal methods for {@link TableResult}. */
@Internal
public interface TableResultInternal extends TableResult {
    TableResultInternal TABLE_RESULT_OK =
            TableResultImpl.builder()
                    .resultKind(ResultKind.SUCCESS)
                    .schema(ResolvedSchema.of(Column.physical("result", DataTypes.STRING())))
                    .data(Collections.singletonList(Row.of("OK")))
                    .build();

    /** Returns an iterator that returns the iterator with the internal row data type. */
    CloseableIterator<RowData> collectInternal();

    RowDataToStringConverter getRowDataToStringConverter();
}
