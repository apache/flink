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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.LongAssert;
import org.assertj.core.api.StringAssert;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Assertions for {@link RowData}. */
@Experimental
public class RowDataAssert extends AbstractAssert<RowDataAssert, RowData> {

    public RowDataAssert(RowData rowData) {
        super(rowData, RowDataAssert.class);
    }

    public RowDataAssert hasKind(RowKind kind) {
        isNotNull();
        assertThat(this.actual.getRowKind()).isEqualTo(kind);
        return this;
    }

    public RowDataAssert hasArity(int arity) {
        isNotNull();
        assertThat(this.actual.getArity()).isEqualTo(arity);
        return this;
    }

    public StringDataAssert getStringData(int index) {
        isNotNullAt(index);
        return new StringDataAssert(this.actual.getString(index));
    }

    public StringAssert getString(int index) {
        return getStringData(index).asString();
    }

    public LongAssert getLong(int index) {
        isNotNullAt(index);
        return new LongAssert(this.actual.getLong(index));
    }

    public RowDataAssert isNullAt(int index) {
        isNotNull();
        assertThat(this.actual.isNullAt(index)).isTrue();
        return this;
    }

    public RowDataAssert isNotNullAt(int index) {
        isNotNull();
        assertThat(this.actual.isNullAt(index)).isFalse();
        return this;
    }

    public RowDataAssert asGeneric(DataType dataType) {
        return asGeneric(dataType.getLogicalType());
    }

    public RowDataAssert asGeneric(LogicalType logicalType) {
        GenericRowData actual = InternalDataUtils.toGenericRow(this.actual, logicalType);
        return new RowDataAssert(actual)
                .usingComparator(
                        (x, y) -> {
                            // Avoid converting actual again
                            x = x == actual ? x : InternalDataUtils.toGenericRow(x, logicalType);
                            y = y == actual ? y : InternalDataUtils.toGenericRow(y, logicalType);
                            if (Objects.equals(x, y)) {
                                return 0;
                            }
                            return Objects.hashCode(x) < Objects.hashCode(y) ? -1 : 1;
                        });
    }

    /** In order to execute this assertion, you need flink-table-runtime in the classpath. */
    public RowAssert asRow(DataType dataType) {
        return new RowAssert(
                InternalDataUtils.resolveToExternalOrNull(dataType).apply(this.actual));
    }
}
