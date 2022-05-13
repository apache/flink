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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.utils.EncodingUtils;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.lang.UProperty;

import javax.annotation.Nullable;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Print the result and content as tableau form.
 *
 * <p>For example: (printRowKind is true)
 *
 * <pre>
 * +----+-------------+---------+-------------+
 * | op | boolean_col | int_col | varchar_col |
 * +----+-------------+---------+-------------+
 * | +I |        true |       1 |         abc |
 * | -U |       false |       2 |         def |
 * | +U |       false |       3 |         def |
 * | -D |      &lt;NULL&gt; |  &lt;NULL&gt; |      &lt;NULL&gt; |
 * +----+-------------+---------+-------------+
 * 4 rows in set
 * </pre>
 */
@Internal
public final class TableauStyle implements PrintStyle {

    // constants for printing
    private static final String ROW_KIND_COLUMN = "op";
    private static final String COLUMN_TRUNCATED_FLAG = "...";

    private final RowDataToStringConverter converter;

    /** The max width of a column. */
    private final int maxColumnWidth;

    /**
     * A flag to indicate whether null should be printed as empty string more than {@code <NULL>}.
     */
    private final boolean printNullAsEmpty;

    /** A flag to indicate whether print row kind info. */
    private final boolean printRowKind;

    private int[] columnWidths;

    private String[] columnNames;

    TableauStyle(
            ResolvedSchema resolvedSchema,
            RowDataToStringConverter converter,
            int[] columnWidths,
            int maxColumnWidth,
            boolean printNullAsEmpty,
            boolean printRowKind) {
        this.converter = converter;
        this.columnWidths = columnWidths;
        this.maxColumnWidth = maxColumnWidth;
        this.printNullAsEmpty = printNullAsEmpty;
        this.printRowKind = printRowKind;

        if (printRowKind) {
            this.columnNames =
                    Stream.concat(
                                    Stream.of(ROW_KIND_COLUMN),
                                    resolvedSchema.getColumnNames().stream())
                            .toArray(String[]::new);
        } else {
            this.columnNames = resolvedSchema.getColumnNames().toArray(new String[0]);
        }
    }

    /** Returns null if the column widths are not precomputed using the row type. */
    public @Nullable int[] getColumnWidths() {
        return columnWidths;
    }

    @Override
    public void print(Iterator<RowData> it, PrintWriter printWriter) {
        if (!it.hasNext()) {
            printWriter.println("Empty set");
            printWriter.flush();
            return;
        }

        if (columnWidths == null) {
            final List<RowData> rows = new ArrayList<>();
            final List<String[]> content = new ArrayList<>();
            content.add(columnNames);
            while (it.hasNext()) {
                RowData row = it.next();
                rows.add(row);
                content.add(rowFieldsToString(row));
            }
            this.columnWidths = columnWidthsByContent(columnNames, content, maxColumnWidth);
            it = rows.iterator();
        }

        // print border line
        printBorderLine(printWriter);
        // print field names
        printColumnNamesTableauRow(printWriter);
        // print border line
        printBorderLine(printWriter);

        long numRows = 0;
        while (it.hasNext()) {
            String[] cols = rowFieldsToString(it.next());

            // print content
            printTableauRow(cols, printWriter);
            numRows++;
        }

        // print border line
        printBorderLine(printWriter);
        final String rowTerm = numRows > 1 ? "rows" : "row";
        printWriter.println(numRows + " " + rowTerm + " in set");
        printWriter.flush();
    }

    public String[] rowFieldsToString(RowData row) {
        final int len = printRowKind ? row.getArity() + 1 : row.getArity();
        final String[] result = new String[len];

        final int offset = printRowKind ? 1 : 0;
        final String[] conversionResult = converter.convert(row);

        if (printRowKind) {
            result[0] = row.getRowKind().shortString();
        }

        for (int i = 0; i < row.getArity(); i++) {
            if (row.isNullAt(i) && printNullAsEmpty) {
                result[i + offset] = "";
            } else {
                result[i + offset] = conversionResult[i];
            }
        }
        return result;
    }

    public void printColumnNamesTableauRow(PrintWriter printWriter) {
        this.printTableauRow(columnNames, printWriter);
    }

    public void printTableauRow(String[] cols, PrintWriter printWriter) {
        if (columnWidths == null) {
            this.columnWidths =
                    columnWidthsByContent(
                            this.columnNames, Collections.singletonList(cols), maxColumnWidth);
        }

        final StringBuilder sb = new StringBuilder();
        sb.append("|");
        int idx = 0;
        for (String col : cols) {
            sb.append(" ");
            int displayWidth = getStringDisplayWidth(col);
            if (displayWidth <= columnWidths[idx]) {
                sb.append(EncodingUtils.repeat(' ', columnWidths[idx] - displayWidth));
                sb.append(col);
            } else {
                sb.append(truncateString(col, columnWidths[idx] - COLUMN_TRUNCATED_FLAG.length()));
                sb.append(COLUMN_TRUNCATED_FLAG);
            }
            sb.append(" |");
            idx++;
        }
        printWriter.println(sb);
        printWriter.flush();
    }

    public void printBorderLine(PrintWriter printWriter) {
        if (columnWidths == null) {
            throw new IllegalStateException(
                    "Column widths should be initialized before printing a border line");
        }

        printWriter.append("+");
        for (int width : columnWidths) {
            printWriter.append(EncodingUtils.repeat('-', width + 1));
            printWriter.append("-+");
        }
        printWriter.println();
    }

    // Package private and private static methods to deal with complexity of string writing and
    // formatting

    /**
     * Try to derive column width based on column types. If result set is not small enough to be
     * stored in java heap memory, we can't determine column widths based on column values.
     */
    static int[] columnWidthsByType(
            List<Column> columns,
            int maxColumnWidth,
            boolean printNullAsEmpty,
            boolean printRowKind) {
        // fill width with field names first
        final int[] colWidths = columns.stream().mapToInt(col -> col.getName().length()).toArray();

        // determine proper column width based on types
        for (int i = 0; i < columns.size(); ++i) {
            LogicalType type = columns.get(i).getDataType().getLogicalType();
            int len;
            switch (type.getTypeRoot()) {
                case TINYINT:
                    len = TinyIntType.PRECISION + 1; // extra for negative value
                    break;
                case SMALLINT:
                    len = SmallIntType.PRECISION + 1; // extra for negative value
                    break;
                case INTEGER:
                    len = IntType.PRECISION + 1; // extra for negative value
                    break;
                case BIGINT:
                    len = BigIntType.PRECISION + 1; // extra for negative value
                    break;
                case DECIMAL:
                    len =
                            ((DecimalType) type).getPrecision()
                                    + 2; // extra for negative value and decimal point
                    break;
                case BOOLEAN:
                    len = 5; // "true" or "false"
                    break;
                case DATE:
                    len = 10; // e.g. 9999-12-31
                    break;
                case TIME_WITHOUT_TIME_ZONE:
                    int precision = ((TimeType) type).getPrecision();
                    len = precision == 0 ? 8 : precision + 9; // 23:59:59[.999999999]
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    precision = ((TimestampType) type).getPrecision();
                    len = timestampTypeColumnWidth(precision);
                    break;
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    precision = ((LocalZonedTimestampType) type).getPrecision();
                    len = timestampTypeColumnWidth(precision);
                    break;
                default:
                    len = maxColumnWidth;
            }

            // adjust column width with potential null values
            len = printNullAsEmpty ? len : Math.max(len, PrintStyle.NULL_VALUE.length());
            colWidths[i] = Math.max(colWidths[i], len);
        }

        // add an extra column for row kind if necessary
        if (printRowKind) {
            final int[] ret = new int[columns.size() + 1];
            ret[0] = ROW_KIND_COLUMN.length();
            System.arraycopy(colWidths, 0, ret, 1, columns.size());
            return ret;
        } else {
            return colWidths;
        }
    }

    private static int[] columnWidthsByContent(
            String[] columnNames, List<String[]> rows, int maxColumnWidth) {
        // fill width with field names first
        final int[] colWidths = Stream.of(columnNames).mapToInt(String::length).toArray();

        // fill column width with real data
        for (String[] row : rows) {
            for (int i = 0; i < row.length; ++i) {
                colWidths[i] = Math.max(colWidths[i], getStringDisplayWidth(row[i]));
            }
        }

        // adjust column width with maximum length
        for (int i = 0; i < colWidths.length; ++i) {
            colWidths[i] = Math.min(colWidths[i], maxColumnWidth);
        }

        return colWidths;
    }

    private static int timestampTypeColumnWidth(int precision) {
        int base = 19; // length of uuuu-MM-dd HH:mm:ss
        if (precision == 0) {
            return base;
        } else if (precision <= 3) {
            // uuuu-MM-dd HH:mm:ss.sss
            return base + 4;
        } else if (precision <= 6) {
            // uuuu-MM-dd HH:mm:ss.sssssss
            return base + 7;
        } else {
            return base + 10;
        }
    }

    static int getStringDisplayWidth(String str) {
        int numOfFullWidthCh = (int) str.codePoints().filter(TableauStyle::isFullWidth).count();
        return str.length() + numOfFullWidthCh;
    }

    /**
     * Check codePoint is FullWidth or not according to Unicode Standard version 12.0.0. See
     * http://unicode.org/reports/tr11/
     */
    static boolean isFullWidth(int codePoint) {
        int value = UCharacter.getIntPropertyValue(codePoint, UProperty.EAST_ASIAN_WIDTH);
        switch (value) {
            case UCharacter.EastAsianWidth.NEUTRAL:
            case UCharacter.EastAsianWidth.AMBIGUOUS:
            case UCharacter.EastAsianWidth.HALFWIDTH:
            case UCharacter.EastAsianWidth.NARROW:
                return false;
            case UCharacter.EastAsianWidth.FULLWIDTH:
            case UCharacter.EastAsianWidth.WIDE:
                return true;
            default:
                throw new RuntimeException("unknown UProperty.EAST_ASIAN_WIDTH: " + value);
        }
    }

    private static String truncateString(String col, int targetWidth) {
        int passedWidth = 0;
        int i = 0;
        for (; i < col.length(); i++) {
            if (isFullWidth(Character.codePointAt(col, i))) {
                passedWidth += 2;
            } else {
                passedWidth += 1;
            }
            if (passedWidth > targetWidth) {
                break;
            }
        }
        String substring = col.substring(0, i);

        // pad with ' ' before the column
        int lackedWidth = targetWidth - getStringDisplayWidth(substring);
        if (lackedWidth > 0) {
            substring = EncodingUtils.repeat(' ', lackedWidth) + substring;
        }
        return substring;
    }
}
