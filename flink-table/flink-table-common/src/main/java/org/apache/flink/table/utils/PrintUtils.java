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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.lang.UProperty;

import javax.annotation.Nullable;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/** Utilities for print formatting. */
@Internal
public class PrintUtils {

    // constants for printing
    public static final int MAX_COLUMN_WIDTH = 30;
    public static final String NULL_COLUMN = "(NULL)";
    public static final String ROW_KIND_COLUMN = "op";
    private static final String COLUMN_TRUNCATED_FLAG = "...";

    private PrintUtils() {}

    /**
     * Displays the result in a tableau form.
     *
     * <p>For example:
     *
     * <pre>
     * +-------------+---------+-------------+
     * | boolean_col | int_col | varchar_col |
     * +-------------+---------+-------------+
     * |        true |       1 |         abc |
     * |       false |       2 |         def |
     * |      (NULL) |  (NULL) |      (NULL) |
     * +-------------+---------+-------------+
     * 3 rows in set
     * </pre>
     */
    public static void printAsTableauForm(
            ResolvedSchema resolvedSchema, Iterator<Row> it, PrintWriter printWriter) {
        printAsTableauForm(
                resolvedSchema, it, printWriter, MAX_COLUMN_WIDTH, NULL_COLUMN, false, false);
    }

    /**
     * Displays the result in a tableau form.
     *
     * <p><b>NOTE:</b> please make sure the data to print is small enough to be stored in java heap
     * memory if the column width is derived from content (`deriveColumnWidthByType` is false).
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
     * | -D |      (NULL) |  (NULL) |      (NULL) |
     * +----+-------------+---------+-------------+
     * 4 rows in set
     * </pre>
     *
     * @param resolvedSchema The schema of the data to print
     * @param it The iterator for the data to print
     * @param printWriter The writer to write to
     * @param maxColumnWidth The max width of a column
     * @param nullColumn The string representation of a null value
     * @param deriveColumnWidthByType A flag to indicate whether the column width is derived from
     *     type (true) or content (false).
     * @param printRowKind A flag to indicate whether print row kind info
     */
    public static void printAsTableauForm(
            ResolvedSchema resolvedSchema,
            Iterator<Row> it,
            PrintWriter printWriter,
            int maxColumnWidth,
            String nullColumn,
            boolean deriveColumnWidthByType,
            boolean printRowKind) {
        if (!it.hasNext()) {
            printWriter.println("Empty set");
            printWriter.flush();
            return;
        }
        final List<Column> columns = resolvedSchema.getColumns();
        String[] columnNames = columns.stream().map(Column::getName).toArray(String[]::new);
        if (printRowKind) {
            columnNames =
                    Stream.concat(Stream.of(ROW_KIND_COLUMN), Arrays.stream(columnNames))
                            .toArray(String[]::new);
        }

        final int[] colWidths;
        if (deriveColumnWidthByType) {
            colWidths =
                    columnWidthsByType(
                            columns,
                            maxColumnWidth,
                            nullColumn,
                            printRowKind ? ROW_KIND_COLUMN : null);
        } else {
            final List<Row> rows = new ArrayList<>();
            final List<String[]> content = new ArrayList<>();
            content.add(columnNames);
            while (it.hasNext()) {
                Row row = it.next();
                rows.add(row);
                content.add(rowToString(row, nullColumn, printRowKind));
            }
            colWidths = columnWidthsByContent(columnNames, content, maxColumnWidth);
            it = rows.iterator();
        }

        final String borderline = PrintUtils.genBorderLine(colWidths);
        // print border line
        printWriter.println(borderline);
        // print field names
        PrintUtils.printSingleRow(colWidths, columnNames, printWriter);
        // print border line
        printWriter.println(borderline);

        long numRows = 0;
        while (it.hasNext()) {
            String[] cols = rowToString(it.next(), nullColumn, printRowKind);

            // print content
            printSingleRow(colWidths, cols, printWriter);
            numRows++;
        }

        // print border line
        printWriter.println(borderline);
        final String rowTerm = numRows > 1 ? "rows" : "row";
        printWriter.println(numRows + " " + rowTerm + " in set");
        printWriter.flush();
    }

    public static String[] rowToString(Row row) {
        return rowToString(row, NULL_COLUMN, false);
    }

    public static String[] rowToString(Row row, String nullColumn, boolean printRowKind) {
        final int len = printRowKind ? row.getArity() + 1 : row.getArity();
        final List<String> fields = new ArrayList<>(len);
        if (printRowKind) {
            fields.add(row.getKind().shortString());
        }
        for (int i = 0; i < row.getArity(); i++) {
            final Object field = row.getField(i);
            if (field == null) {
                fields.add(nullColumn);
            } else {
                fields.add(StringUtils.arrayAwareToString(field));
            }
        }
        return fields.toArray(new String[0]);
    }

    public static String genBorderLine(int[] colWidths) {
        StringBuilder sb = new StringBuilder();
        sb.append("+");
        for (int width : colWidths) {
            sb.append(EncodingUtils.repeat('-', width + 1));
            sb.append("-+");
        }
        return sb.toString();
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

    /**
     * Try to derive column width based on column types. If result set is not small enough to be
     * stored in java heap memory, we can't determine column widths based on column values.
     */
    public static int[] columnWidthsByType(
            List<Column> columns,
            int maxColumnWidth,
            String nullColumn,
            @Nullable String rowKindColumn) {
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
            colWidths[i] = Math.max(colWidths[i], Math.max(len, nullColumn.length()));
        }

        // add an extra column for row kind if necessary
        if (rowKindColumn != null) {
            final int[] ret = new int[columns.size() + 1];
            ret[0] = rowKindColumn.length();
            System.arraycopy(colWidths, 0, ret, 1, columns.size());
            return ret;
        } else {
            return colWidths;
        }
    }

    /**
     * Here we consider two popular class for timestamp: LocalDateTime and java.sql.Timestamp.
     *
     * <p>According to LocalDateTime's comment, the string output will be one of the following
     * ISO-8601 formats:
     * <li>{@code uuuu-MM-dd'T'HH:mm:ss}
     * <li>{@code uuuu-MM-dd'T'HH:mm:ss.SSS}
     * <li>{@code uuuu-MM-dd'T'HH:mm:ss.SSSSSS}
     * <li>{@code uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSS}
     *
     *     <p>And for java.sql.Timestamp, the number of digits after point will be precision except
     *     when precision is 0. In that case, the format would be 'uuuu-MM-dd HH:mm:ss.0'
     */
    private static int timestampTypeColumnWidth(int precision) {
        int base = 19; // length of uuuu-MM-dd HH:mm:ss
        if (precision == 0) {
            return base + 2; // consider java.sql.Timestamp
        } else if (precision <= 3) {
            return base + 4;
        } else if (precision <= 6) {
            return base + 7;
        } else {
            return base + 10;
        }
    }

    public static void printSingleRow(int[] colWidths, String[] cols, PrintWriter printWriter) {
        StringBuilder sb = new StringBuilder();
        sb.append("|");
        int idx = 0;
        for (String col : cols) {
            sb.append(" ");
            int displayWidth = getStringDisplayWidth(col);
            if (displayWidth <= colWidths[idx]) {
                sb.append(EncodingUtils.repeat(' ', colWidths[idx] - displayWidth));
                sb.append(col);
            } else {
                sb.append(truncateString(col, colWidths[idx] - COLUMN_TRUNCATED_FLAG.length()));
                sb.append(COLUMN_TRUNCATED_FLAG);
            }
            sb.append(" |");
            idx++;
        }
        printWriter.println(sb.toString());
        printWriter.flush();
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

    public static int getStringDisplayWidth(String str) {
        int numOfFullWidthCh = (int) str.codePoints().filter(PrintUtils::isFullWidth).count();
        return str.length() + numOfFullWidthCh;
    }

    /**
     * Check codePoint is FullWidth or not according to Unicode Standard version 12.0.0. See
     * http://unicode.org/reports/tr11/
     */
    public static boolean isFullWidth(int codePoint) {
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
}
