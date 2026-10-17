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

package org.apache.flink.table.planner.parse;

import org.apache.flink.configuration.GlobalConfiguration;

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Optional;

/**
 * Renders the line of a SQL text that a parser position points at, with carets under the span, the
 * way psql shows a syntax error.
 *
 * <p>A long line is cut to a window around the span that fits an 80-column terminal. Sensitive
 * option values on the line are masked and keep their length, so the columns stay valid; a value on
 * the next line, or a credential inside another value such as a URL, is not. A tab becomes one
 * space because the parser counts it as one column. Widths are display cells, so East Asian
 * characters count two.
 */
final class ParseErrorSnippet {

    private static final int WIDTH = 76;
    private static final int RIGHT_CONTEXT = 10;
    private static final String INDENT = "    ";
    private static final String CUT = "...";
    private static final String SEPARATOR_CHARS = "=:>";
    private static final String QUOTES = "'\"`";

    private ParseErrorSnippet() {}

    /**
     * Returns the indented line and the caret line under it, or nothing when the position is
     * unknown or lies beyond the text. Values of options whose key is sensitive, built in or
     * through {@code additionalSensitiveKeys}, are masked.
     */
    static Optional<String> render(
            String sql, SqlParserPos pos, List<String> additionalSensitiveKeys) {
        if (pos.getLineNum() <= 0) {
            return Optional.empty();
        }
        final int lineStart = lineStart(sql, pos.getLineNum());
        if (lineStart < 0) {
            return Optional.empty();
        }
        String line =
                mask(sql.substring(lineStart, lineEnd(sql, lineStart)), additionalSensitiveKeys)
                        .replace('\t', ' ');
        final int caret = Math.min(Math.max(pos.getColumnNum() - 1, 0), line.length());
        // Trailing whitespace goes, except where the position points into it.
        line = line.substring(0, Math.max(line.stripTrailing().length(), caret));
        final int spanEnd =
                pos.getEndLineNum() == pos.getLineNum()
                        ? Math.min(
                                line.length(),
                                caret + pos.getEndColumnNum() - pos.getColumnNum() + 1)
                        : line.length();

        // The window is measured in cells; the caret may sit one cell past the end of the line.
        final int[] cells = cellOffsets(line);
        final int extent = Math.max(cells[line.length()], cells[caret] + 1);
        int fromCell = 0;
        int toCell = extent;
        String head = "";
        String tail = "";
        if (extent > WIDTH) {
            final int needed = Math.min(extent, cells[caret] + RIGHT_CONTEXT + 1);
            if (needed <= WIDTH - CUT.length()) {
                toCell = WIDTH - CUT.length();
                tail = CUT;
            } else if (needed == extent) {
                fromCell = extent - (WIDTH - CUT.length());
                head = CUT;
            } else {
                toCell = needed;
                fromCell = toCell - (WIDTH - 2 * CUT.length());
                head = CUT;
                tail = CUT;
            }
        }
        final int from = firstBoundaryAtOrAfter(line, cells, fromCell);
        final int to = lastBoundaryAtOrBefore(line, cells, toCell);
        final int caretEnd = Math.min(Math.max(spanEnd, caret + 1), Math.max(to, caret));
        return Optional.of(
                INDENT
                        + head
                        + line.substring(from, to)
                        + tail
                        + "\n"
                        + INDENT
                        + " ".repeat(head.length() + cells[caret] - cells[from])
                        + "^".repeat(Math.max(cells[caretEnd] - cells[caret], 1)));
    }

    /**
     * The index where the given line starts, or -1 when the text has fewer lines. A lone CR is a
     * line break for the lexer as well, which Calcite's {@code SqlParserUtil.nextLine} gets wrong.
     */
    private static int lineStart(String sql, int line) {
        int start = 0;
        for (int i = 1; i < line; i++) {
            final int end = lineEnd(sql, start);
            if (end >= sql.length()) {
                return -1;
            }
            start = end + (sql.startsWith("\r\n", end) ? 2 : 1);
        }
        return start;
    }

    private static int lineEnd(String sql, int start) {
        int end = start;
        while (end < sql.length() && sql.charAt(end) != '\n' && sql.charAt(end) != '\r') {
            end++;
        }
        return end;
    }

    /** Display cells before each character index, so {@code cells[line.length()]} is the width. */
    private static int[] cellOffsets(String line) {
        final int[] cells = new int[line.length() + 1];
        int width = 0;
        for (int i = 0; i < line.length(); ) {
            final int cp = line.codePointAt(i);
            final int count = Character.charCount(cp);
            for (int j = 0; j < count; j++) {
                cells[i + j] = width;
            }
            width += cellsOf(cp);
            i += count;
        }
        cells[line.length()] = width;
        return cells;
    }

    private static int firstBoundaryAtOrAfter(String line, int[] cells, int cell) {
        int i = 0;
        while (i < line.length() && (cells[i] < cell || !isBoundary(line, i))) {
            i++;
        }
        return i;
    }

    private static int lastBoundaryAtOrBefore(String line, int[] cells, int cell) {
        int i = line.length();
        while (i > 0 && (cells[i] > cell || !isBoundary(line, i))) {
            i--;
        }
        return i;
    }

    /** A cut may not split a surrogate pair or separate a mark from its base. */
    private static boolean isBoundary(String line, int i) {
        if (i == 0 || i == line.length()) {
            return true;
        }
        return !Character.isLowSurrogate(line.charAt(i)) && cellsOf(line.codePointAt(i)) > 0;
    }

    /**
     * Masks the value of every {@code key = value} pair whose key is sensitive. A key is a string
     * in single, double or back quotes, or a bare token that starts with a letter or digit and
     * contains a dot or a dash, which an unquoted SQL identifier cannot; the separator is any run
     * of {@code =}, {@code :} or {@code >}, or nothing at all; the value is a quoted string,
     * unterminated included, or after a separator a bare token.
     */
    private static String mask(String line, List<String> additionalSensitiveKeys) {
        final StringBuilder masked = new StringBuilder(line);
        final int length = line.length();
        int i = 0;
        while (i < length) {
            final String key;
            final int keyEnd;
            if (QUOTES.indexOf(line.charAt(i)) >= 0) {
                final int end = quotedEnd(line, i);
                key = line.substring(i + 1, end < 0 ? length : end - 1);
                keyEnd = end < 0 ? length : end;
            } else if (Character.isLetterOrDigit(line.charAt(i))) {
                int end = i;
                while (end < length && isBareKeyChar(line.charAt(end))) {
                    end++;
                }
                key = line.substring(i, end);
                keyEnd = end;
                if (key.indexOf('.') < 0 && key.indexOf('-') < 0) {
                    i = end;
                    continue;
                }
            } else {
                i++;
                continue;
            }

            int j = skipSpaces(line, keyEnd);
            final int separatorStart = j;
            while (j < length && SEPARATOR_CHARS.indexOf(line.charAt(j)) >= 0) {
                j++;
            }
            final boolean separated = j > separatorStart;
            j = skipSpaces(line, j);

            final int valueStart;
            final int valueEnd;
            final int next;
            if (j < length && QUOTES.indexOf(line.charAt(j)) >= 0) {
                final int end = quotedEnd(line, j);
                valueStart = j + 1;
                valueEnd = end < 0 ? length : end - 1;
                next = end < 0 ? length : end;
            } else if (separated && j < length && isBareValueChar(line.charAt(j))) {
                valueStart = j;
                int end = j;
                while (end < length && isBareValueChar(line.charAt(end))) {
                    end++;
                }
                valueEnd = end;
                next = end;
            } else {
                i = keyEnd;
                continue;
            }
            if (GlobalConfiguration.isSensitive(key, additionalSensitiveKeys)) {
                for (int k = valueStart; k < valueEnd; k++) {
                    masked.setCharAt(k, '*');
                }
            }
            i = next;
        }
        return masked.toString();
    }

    /** The index after the closing quote of the string starting at {@code start}, or -1. */
    private static int quotedEnd(String line, int start) {
        final char quote = line.charAt(start);
        int i = start + 1;
        while (i < line.length()) {
            if (line.charAt(i) == quote) {
                if (i + 1 < line.length() && line.charAt(i + 1) == quote) {
                    i += 2;
                    continue;
                }
                return i + 1;
            }
            i++;
        }
        return -1;
    }

    private static int skipSpaces(String line, int i) {
        while (i < line.length() && Character.isWhitespace(line.charAt(i))) {
            i++;
        }
        return i;
    }

    private static boolean isBareKeyChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '.' || c == '-';
    }

    private static boolean isBareValueChar(char c) {
        return !Character.isWhitespace(c) && ",;()".indexOf(c) < 0 && QUOTES.indexOf(c) < 0;
    }

    /** Display cells of a code point: marks and format characters none, East Asian wide two. */
    private static int cellsOf(int cp) {
        final int type = Character.getType(cp);
        if (type == Character.NON_SPACING_MARK
                || type == Character.ENCLOSING_MARK
                || type == Character.FORMAT) {
            return 0;
        }
        return isWide(cp) ? 2 : 1;
    }

    private static boolean isWide(int cp) {
        return (cp >= 0x1100 && cp <= 0x115F)
                || (cp >= 0x2E80 && cp <= 0x303E)
                || (cp >= 0x3041 && cp <= 0x33FF)
                || (cp >= 0x3400 && cp <= 0x4DBF)
                || (cp >= 0x4E00 && cp <= 0x9FFF)
                || (cp >= 0xA000 && cp <= 0xA4CF)
                || (cp >= 0xAC00 && cp <= 0xD7A3)
                || (cp >= 0xF900 && cp <= 0xFAFF)
                || (cp >= 0xFE30 && cp <= 0xFE4F)
                || (cp >= 0xFF00 && cp <= 0xFF60)
                || (cp >= 0xFFE0 && cp <= 0xFFE6)
                || (cp >= 0x1F300 && cp <= 0x1FAFF)
                || (cp >= 0x20000 && cp <= 0x3FFFD);
    }
}
