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

package org.apache.flink.table.client.cli.parser;

import org.jline.utils.AttributedStyle;

/** Styles for {@link SqlClientSyntaxHighlighter}. */
public class SyntaxHighlightStyle {

    /**
     * Pre-defined highlight styles.
     *
     * <p>The {@link #CHESTER}, {@link #DRACULA}, {@link #SOLARIZED} and {@link #VS2010} styles are
     * inspired by <a href="https://github.com/Gillisdc/sqldeveloper-syntax-highlighting">Gillis's
     * Colour schemes for Oracle SQL Developer</a> (not the same but more or less similar).
     *
     * <p>Similarly, the {@link #OBSIDIAN} style is inspired by <a
     * href="https://github.com/ozmoroz/ozbsidian-sqldeveloper">ozmoroz's OzBsidian colour scheme
     * for Oracle SQL Developer</a>.
     *
     * <p>Similarly, the {@link #GESHI} style is inspired by <a
     * href="https://github.com/GeSHi/geshi-1.0/blob/master/src/geshi/sql.php">GeSHi - Generic
     * Syntax Highlighter for sql</a>
     */
    public enum BuiltInStyle {
        DEFAULT(
                AttributedStyle.DEFAULT,
                AttributedStyle.DEFAULT,
                AttributedStyle.DEFAULT,
                AttributedStyle.DEFAULT,
                AttributedStyle.DEFAULT,
                AttributedStyle.DEFAULT),
        DARK(BOLD_BLUE, WHITE, ITALIC_BRIGHT, BOLD_BRIGHT, GREEN, CYAN),
        LIGHT(BOLD_RED, BLACK, ITALIC_BRIGHT, BOLD_BRIGHT, GREEN, CYAN),
        CHESTER(BOLD_BLUE, WHITE, ITALIC_GREEN, BOLD_GREEN, RED, CYAN),
        DRACULA(BOLD_MAGENTA, WHITE, ITALIC_CYAN, BOLD_CYAN, GREEN, RED),
        SOLARIZED(BOLD_YELLOW, BLUE, ITALIC_BRIGHT, BOLD_BRIGHT, GREEN, RED),
        VS2010(BOLD_BLUE, WHITE, ITALIC_GREEN, BOLD_GREEN, RED, MAGENTA),
        OBSIDIAN(BOLD_GREEN, WHITE, ITALIC_BRIGHT, BOLD_BRIGHT, RED, MAGENTA),
        GESHI(
                AttributedStyle.BOLD.foreground(0x99, 0x33, 0x33),
                WHITE,
                AttributedStyle.DEFAULT.italic().foreground(0x80, 0x80, 0x80),
                AttributedStyle.DEFAULT.bold().foreground(0x80, 0x80, 0x80),
                AttributedStyle.DEFAULT.foreground(0x66, 0xCC, 0x66),
                AttributedStyle.DEFAULT.foreground(0x0, 0x0, 0x99));
        private final SyntaxHighlightStyle style;

        BuiltInStyle(
                AttributedStyle keywordStyle,
                AttributedStyle defaultStyle,
                AttributedStyle commentStyle,
                AttributedStyle hintStyle,
                AttributedStyle singleQuotedStyle,
                AttributedStyle sqlIdentifierStyle) {
            style =
                    new SyntaxHighlightStyle(
                            commentStyle,
                            hintStyle,
                            defaultStyle,
                            keywordStyle,
                            singleQuotedStyle,
                            sqlIdentifierStyle);
        }

        public SyntaxHighlightStyle getHighlightStyle() {
            return style;
        }

        public static SyntaxHighlightStyle.BuiltInStyle fromString(String styleName) {
            for (SyntaxHighlightStyle.BuiltInStyle style : values()) {
                if (style.name().equalsIgnoreCase(styleName)) {
                    return style;
                }
            }
            // in case of wrong name fallback to default
            return DEFAULT;
        }
    }

    private final AttributedStyle commentStyle;
    private final AttributedStyle hintStyle;
    private final AttributedStyle defaultStyle;
    private final AttributedStyle keywordStyle;
    private final AttributedStyle singleQuotedStyle;
    private final AttributedStyle sqlIdentifierStyle;

    public SyntaxHighlightStyle(
            AttributedStyle commentStyle,
            AttributedStyle hintStyle,
            AttributedStyle defaultStyle,
            AttributedStyle keywordStyle,
            AttributedStyle singleQuotedStyle,
            AttributedStyle sqlIdentifierStyle) {
        this.commentStyle = commentStyle;
        this.hintStyle = hintStyle;
        this.defaultStyle = defaultStyle;
        this.keywordStyle = keywordStyle;
        this.singleQuotedStyle = singleQuotedStyle;
        this.sqlIdentifierStyle = sqlIdentifierStyle;
    }

    /**
     * Returns the style for a SQL keyword such as {@code SELECT} or {@code ON}.
     *
     * @return Style for SQL keywords
     */
    public AttributedStyle getKeywordStyle() {
        return keywordStyle;
    }

    /**
     * Returns the style for a SQL character literal, such as {@code 'Hello, world!'}.
     *
     * @return Style for SQL character literals
     */
    public AttributedStyle getQuotedStyle() {
        return singleQuotedStyle;
    }

    /**
     * Returns the style for a SQL identifier, such as {@code `My_table`} or {@code `My table`}.
     *
     * @return Style for SQL identifiers
     */
    public AttributedStyle getSqlIdentifierStyle() {
        return sqlIdentifierStyle;
    }

    /**
     * Returns the style for a SQL comments, such as {@literal /* This is a comment *}{@literal /}
     * or {@literal -- End of line comment}.
     *
     * @return Style for SQL comments
     */
    public AttributedStyle getCommentStyle() {
        return commentStyle;
    }

    /**
     * Returns the style for a SQL hint, such as {@literal /*+ This is a hint *}{@literal /}.
     *
     * @return Style for SQL hints
     */
    public AttributedStyle getHintStyle() {
        return hintStyle;
    }

    /**
     * Returns the style for text that does not match any other style.
     *
     * @return Default style
     */
    public AttributedStyle getDefaultStyle() {
        return defaultStyle;
    }

    // --------------------------------------------------------------------------------------------

    static final AttributedStyle GREEN = AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN);
    static final AttributedStyle CYAN = AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN);
    static final AttributedStyle WHITE = AttributedStyle.DEFAULT.foreground(AttributedStyle.WHITE);
    static final AttributedStyle BLACK = AttributedStyle.DEFAULT.foreground(AttributedStyle.BLACK);
    static final AttributedStyle MAGENTA =
            AttributedStyle.DEFAULT.foreground(AttributedStyle.MAGENTA);
    static final AttributedStyle RED = AttributedStyle.DEFAULT.foreground(AttributedStyle.RED);
    static final AttributedStyle BLUE = AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE);

    static final AttributedStyle BOLD_GREEN =
            AttributedStyle.BOLD.foreground(AttributedStyle.GREEN);
    static final AttributedStyle BOLD_CYAN = AttributedStyle.BOLD.foreground(AttributedStyle.CYAN);
    static final AttributedStyle BOLD_BRIGHT =
            AttributedStyle.BOLD.foreground(AttributedStyle.BRIGHT);
    static final AttributedStyle BOLD_YELLOW =
            AttributedStyle.BOLD.foreground(AttributedStyle.YELLOW);
    static final AttributedStyle BOLD_MAGENTA =
            AttributedStyle.BOLD.foreground(AttributedStyle.MAGENTA);
    static final AttributedStyle BOLD_RED = AttributedStyle.BOLD.foreground(AttributedStyle.RED);
    static final AttributedStyle BOLD_BLUE = AttributedStyle.BOLD.foreground(AttributedStyle.BLUE);
    static final AttributedStyle ITALIC_GREEN =
            AttributedStyle.DEFAULT.italic().foreground(AttributedStyle.GREEN);
    static final AttributedStyle ITALIC_CYAN =
            AttributedStyle.DEFAULT.italic().foreground(AttributedStyle.CYAN);
    static final AttributedStyle ITALIC_BRIGHT =
            AttributedStyle.DEFAULT.italic().foreground(AttributedStyle.BRIGHT);
}
