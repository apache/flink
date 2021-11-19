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

package org.apache.flink.table.utils.print;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.PrintWriter;
import java.util.Iterator;

/** A {@link PrintStyle} defines style and formatting to print a collection of {@link RowData}. */
@Internal
public interface PrintStyle {

    /** Default max column width. */
    int DEFAULT_MAX_COLUMN_WIDTH = 30;

    /** Value used to print {@code null}. */
    String NULL_VALUE = "<NULL>";

    /**
     * Displays the result.
     *
     * @param it The iterator for the data to print
     * @param printWriter The writer to write to
     */
    void print(Iterator<RowData> it, PrintWriter printWriter);

    /**
     * Create a new {@link TableauStyle} using column widths computed from the type.
     *
     * @param schema the schema of the data to print
     * @param converter the converter to use to convert field values to string
     * @param maxColumnWidth Max column width
     * @param printNullAsEmpty A flag to indicate whether null should be printed as empty string
     *     more than {@code <NULL>}
     * @param printRowKind A flag to indicate whether print row kind info.
     */
    static TableauStyle tableauWithTypeInferredColumnWidths(
            ResolvedSchema schema,
            RowDataToStringConverter converter,
            int maxColumnWidth,
            boolean printNullAsEmpty,
            boolean printRowKind) {
        Preconditions.checkArgument(maxColumnWidth > 0, "maxColumnWidth should be greater than 0");
        return new TableauStyle(
                schema,
                converter,
                TableauStyle.columnWidthsByType(
                        schema.getColumns(), maxColumnWidth, printNullAsEmpty, printRowKind),
                maxColumnWidth,
                printNullAsEmpty,
                printRowKind);
    }

    /**
     * Like {@link #tableauWithTypeInferredColumnWidths(ResolvedSchema, RowDataToStringConverter,
     * int, boolean, boolean)}, but uses the data to infer the column size.
     *
     * <p><b>NOTE:</b> please make sure the data to print is small enough to be stored in java heap
     * memory.
     */
    static TableauStyle tableauWithDataInferredColumnWidths(
            ResolvedSchema schema,
            RowDataToStringConverter converter,
            int maxColumnWidth,
            boolean printNullAsEmpty,
            boolean printRowKind) {
        Preconditions.checkArgument(maxColumnWidth > 0, "maxColumnWidth should be greater than 0");
        return new TableauStyle(
                schema, converter, null, maxColumnWidth, printNullAsEmpty, printRowKind);
    }

    /**
     * Like {@link #tableauWithDataInferredColumnWidths(ResolvedSchema, RowDataToStringConverter,
     * int, boolean, boolean)}, but using default values.
     *
     * <p><b>NOTE:</b> please make sure the data to print is small enough to be stored in java heap
     * memory.
     */
    static TableauStyle tableauWithDataInferredColumnWidths(
            ResolvedSchema schema, RowDataToStringConverter converter) {
        return PrintStyle.tableauWithDataInferredColumnWidths(
                schema, converter, DEFAULT_MAX_COLUMN_WIDTH, false, false);
    }

    /**
     * Create a raw content print style, which only print the result content as raw form. column
     * delimiter is ",", row delimiter is "\n".
     */
    static RawContentStyle rawContent(RowDataToStringConverter converter) {
        return new RawContentStyle(converter);
    }
}
