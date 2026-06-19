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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Entrypoint for all the various table assertions. */
@Experimental
public class TableAssertions {

    private TableAssertions() {}

    // --- External data structures

    public static RowAssert assertThat(Row row) {
        return new RowAssert(row);
    }

    // --- Internal data structures

    public static ArrayDataAssert assertThat(ArrayData actual) {
        return new ArrayDataAssert(actual);
    }

    public static MapDataAssert assertThat(MapData actual) {
        return new MapDataAssert(actual);
    }

    public static RowDataAssert assertThat(RowData actual) {
        return new RowDataAssert(actual);
    }

    public static RowDataListAssert assertThatRows(Iterator<RowData> actual) {
        return new RowDataListAssert(
                StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(actual, Spliterator.ORDERED),
                                false)
                        .collect(Collectors.toList()));
    }

    public static RowDataListAssert assertThatRows(Iterable<RowData> actual) {
        if (actual instanceof List) {
            return new RowDataListAssert((List<RowData>) actual);
        }
        return new RowDataListAssert(
                StreamSupport.stream(actual.spliterator(), false).collect(Collectors.toList()));
    }

    public static RowDataListAssert assertThatRows(Stream<RowData> actual) {
        return new RowDataListAssert(actual.collect(Collectors.toList()));
    }

    public static RowDataListAssert assertThatRows(RowData... rows) {
        return new RowDataListAssert(Arrays.asList(rows));
    }

    public static StringDataAssert assertThat(StringData actual) {
        return new StringDataAssert(actual);
    }

    /**
     * Create an assertion for internal data, converted to Generic data structures supporting
     * equality assertions. Use this method when the type can be different.
     *
     * <p>If you need to assert a data always of the same type, use the other assertions like {@link
     * #assertThat(RowData)} instead.
     */
    public static AbstractAssert<?, ?> assertThatGenericDataOfType(
            Object actual, LogicalType logicalType) {
        if (actual instanceof ArrayData) {
            return new ArrayDataAssert((ArrayData) actual).asGeneric(logicalType);
        } else if (actual instanceof MapData) {
            return new MapDataAssert((MapData) actual).asGeneric(logicalType);
        } else if (actual instanceof RowData) {
            return new RowDataAssert((RowData) actual).asGeneric(logicalType);
        } else if (actual instanceof StringData) {
            return new StringDataAssert((StringData) actual);
        }

        return Assertions.assertThatObject(actual);
    }

    public static AbstractAssert<?, ?> assertThatGenericDataOfType(
            Object actual, DataType dataType) {
        return assertThatGenericDataOfType(actual, dataType.getLogicalType());
    }

    // --- Types

    public static DataTypeAssert assertThat(DataType actual) {
        return new DataTypeAssert(actual);
    }

    public static LogicalTypeAssert assertThat(LogicalType actual) {
        return new LogicalTypeAssert(actual);
    }
}
