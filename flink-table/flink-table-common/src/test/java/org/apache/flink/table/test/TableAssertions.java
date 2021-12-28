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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

/** Entrypoint for all the various table assertions. */
public class TableAssertions {

    private TableAssertions() {}

    // --- Internal data structures

    public static RowDataAssert assertThat(RowData actual) {
        return new RowDataAssert(actual);
    }

    public static StringDataAssert assertThat(StringData actual) {
        return new StringDataAssert(actual);
    }

    // --- Types

    public static DataTypeAssert assertThat(DataType actual) {
        return new DataTypeAssert(actual);
    }

    public static LogicalTypeAssert assertThat(LogicalType actual) {
        return new LogicalTypeAssert(actual);
    }
}
