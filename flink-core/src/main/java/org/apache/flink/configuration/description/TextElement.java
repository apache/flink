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

package org.apache.flink.configuration.description;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

/** Represents a text block in the {@link Description}. */
public class TextElement implements BlockElement, InlineElement {
    private final String format;
    private final List<InlineElement> elements;
    private final EnumSet<TextStyle> textStyles = EnumSet.noneOf(TextStyle.class);

    /**
     * Creates a block of text with placeholders ("%s") that will be replaced with proper string
     * representation of given {@link InlineElement}. For example:
     *
     * <p>{@code text("This is a text with a link %s", link("https://somepage", "to here"))}
     *
     * @param format text with placeholders for elements
     * @param elements elements to be put in the text
     * @return block of text
     */
    public static TextElement text(String format, InlineElement... elements) {
        return new TextElement(format, Arrays.asList(elements));
    }

    /**
     * Creates a simple block of text.
     *
     * @param text a simple block of text
     * @return block of text
     */
    public static TextElement text(String text) {
        return new TextElement(text, Collections.emptyList());
    }

    /**
     * Creates a block of text formatted as code.
     *
     * @param text a block of text that will be formatted as code
     * @return block of text formatted as code
     */
    public static TextElement code(String text) {
        TextElement element = text(text);
        element.textStyles.add(TextStyle.CODE);
        return element;
    }

    public String getFormat() {
        return format;
    }

    public List<InlineElement> getElements() {
        return elements;
    }

    public EnumSet<TextStyle> getStyles() {
        return textStyles;
    }

    private TextElement(String format, List<InlineElement> elements) {
        this.format = format;
        this.elements = elements;
    }

    @Override
    public void format(Formatter formatter) {
        formatter.format(this);
    }

    /** Styles that can be applied to {@link TextElement} e.g. code, bold etc. */
    public enum TextStyle {
        CODE
    }
}
