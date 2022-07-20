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

package org.apache.flink.table.test;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ListAssert;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Assertions for {@link List} of {@link RowData}. */
@Experimental
public class RowDataListAssert
        extends AbstractListAssert<RowDataListAssert, List<RowData>, RowData, RowDataAssert> {

    public RowDataListAssert(List<RowData> rowDataList) {
        super(rowDataList, RowDataListAssert.class);
    }

    @Override
    protected RowDataAssert toAssert(RowData value, String description) {
        return new RowDataAssert(value).as(description);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    protected RowDataListAssert newAbstractIterableAssert(Iterable<? extends RowData> iterable) {
        if (iterable instanceof List) {
            return new RowDataListAssert((List<RowData>) iterable);
        }
        return new RowDataListAssert(
                StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList()));
    }

    public RowDataListAssert asGeneric(DataType dataType) {
        return asGeneric(dataType.getLogicalType());
    }

    public RowDataListAssert asGeneric(LogicalType logicalType) {
        return usingElementComparator(
                (x, y) -> {
                    x = InternalDataUtils.toGenericRow(x, logicalType);
                    y = InternalDataUtils.toGenericRow(y, logicalType);
                    if (Objects.equals(x, y)) {
                        return 0;
                    }
                    return Objects.hashCode(x) < Objects.hashCode(y) ? -1 : 1;
                });
    }

    /** In order to execute this assertion, you need flink-table-runtime in the classpath. */
    public ListAssert<Row> asRows(DataType dataType) {
        return new ListAssert<>(
                this.actual.stream().map(InternalDataUtils.resolveToExternalOrNull(dataType)));
    }
}
