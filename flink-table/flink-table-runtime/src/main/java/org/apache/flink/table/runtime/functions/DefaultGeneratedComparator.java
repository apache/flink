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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.GeneratedComparator;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;

/**
 * Default runtime implementation for {@link GeneratedComparator}, backed by a generated {@link
 * RecordComparator}. A value that is not a row is compared as a row of one field.
 */
@Internal
public final class DefaultGeneratedComparator implements GeneratedComparator {

    private static final long serialVersionUID = 1L;

    private final GeneratedRecordComparator generated;

    private final boolean isRow;

    private transient RecordComparator comparator;

    private transient GenericRowData leftRow;

    private transient GenericRowData rightRow;

    public DefaultGeneratedComparator(
            final GeneratedRecordComparator generated, final boolean isRow) {
        this.generated = generated;
        this.isRow = isRow;
    }

    @Override
    public void open(final FunctionContext context) {
        comparator = generated.newInstance(context.getUserCodeClassLoader());
        leftRow = new GenericRowData(1);
        rightRow = new GenericRowData(1);
    }

    @Override
    public int compare(final Object left, final Object right) {
        if (isRow) {
            return comparator.compare((RowData) left, (RowData) right);
        }
        leftRow.setField(0, left);
        rightRow.setField(0, right);
        return comparator.compare(leftRow, rightRow);
    }
}
