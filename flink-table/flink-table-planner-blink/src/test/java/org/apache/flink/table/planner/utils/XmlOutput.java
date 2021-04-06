/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.utils;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

/**
 * This class is copied from Calcite's {@link org.apache.calcite.util.XmlOutput}, can be removed
 * once we bump to Calcite v1.22.0. The change is in the commit id
 * 1745f752561be04ae34d1fa08593c2d3ba4470e8 in Calcite which removes the new line in {@link
 * #cdata(String, boolean)}.
 *
 * <p>Streaming XML output.
 *
 * <p>Use this class to write XML to any streaming source. While the class itself is unstructured
 * and doesn't enforce any DTD specification, use of the class does ensure that the output is
 * syntactically valid XML.
 */
public class XmlOutput {

    // This Writer is the underlying output stream to which all XML is
    // written.
    private final PrintWriter out;

    // The tagStack is maintained to check that tags are balanced.
    private final Deque<String> tagStack = new ArrayDeque<>();

    // The class maintains an indentation level to improve output quality.
    private int indent;

    // The class also maintains the total number of tags written.  This
    // is used to monitor changes to the output
    private int tagsWritten;

    // This flag is set to true if the output should be compacted.
    // Compacted output is free of extraneous whitespace and is designed
    // for easier transport.
    private boolean compact;

    /** @see #setIndentString */
    private String indentString = "\t";

    /** @see #setGlob */
    private boolean glob;

    /**
     * Whether we have started but not finished a start tag. This only happens if <code>glob</code>
     * is true. The start tag is automatically closed when we start a child node. If there are no
     * child nodes, {@link #endTag} creates an empty tag.
     */
    private boolean inTag;

    /** @see #setAlwaysQuoteCData */
    private boolean alwaysQuoteCData;

    /** @see #setIgnorePcdata */
    private boolean ignorePcdata;

    /**
     * Private helper function to display a degree of indentation.
     *
     * @param out the PrintWriter to which to display output.
     * @param indent the degree of indentation.
     */
    private void displayIndent(PrintWriter out, int indent) {
        if (!compact) {
            for (int i = 0; i < indent; i++) {
                out.print(indentString);
            }
        }
    }

    /**
     * Constructs a new XmlOutput based on any {@link Writer}.
     *
     * @param out the writer to which this XmlOutput generates results.
     */
    public XmlOutput(Writer out) {
        this(new PrintWriter(out, true));
    }

    /**
     * Constructs a new XmlOutput based on a {@link PrintWriter}.
     *
     * @param out the writer to which this XmlOutput generates results.
     */
    public XmlOutput(PrintWriter out) {
        this.out = out;
        indent = 0;
        tagsWritten = 0;
    }

    /**
     * Sets or unsets the compact mode. Compact mode causes the generated XML to be free of
     * extraneous whitespace and other unnecessary characters.
     *
     * @param compact true to turn on compact mode, or false to turn it off.
     */
    public void setCompact(boolean compact) {
        this.compact = compact;
    }

    public boolean getCompact() {
        return compact;
    }

    /**
     * Sets the string to print for each level of indentation. The default is a tab. The value must
     * not be <code>null</code>. Set this to the empty string to achieve no indentation (note that
     * <code>{@link #setCompact}(true)</code> removes indentation <em>and</em> newlines).
     */
    public void setIndentString(String indentString) {
        this.indentString = indentString;
    }

    /** Sets whether to detect that tags are empty. */
    public void setGlob(boolean glob) {
        this.glob = glob;
    }

    /**
     * Sets whether to always quote cdata segments (even if they don't contain special characters).
     */
    public void setAlwaysQuoteCData(boolean alwaysQuoteCData) {
        this.alwaysQuoteCData = alwaysQuoteCData;
    }

    /** Sets whether to ignore unquoted text, such as whitespace. */
    public void setIgnorePcdata(boolean ignorePcdata) {
        this.ignorePcdata = ignorePcdata;
    }

    public boolean getIgnorePcdata() {
        return ignorePcdata;
    }

    /**
     * Sends a string directly to the output stream, without escaping any characters. Use with
     * caution!
     */
    public void print(String s) {
        out.print(s);
    }

    /**
     * Starts writing a new tag to the stream. The tag's name must be given and its attributes
     * should be specified by a fully constructed AttrVector object.
     *
     * @param tagName the name of the tag to write.
     * @param attributes an XMLAttrVector containing the attributes to include in the tag.
     */
    public void beginTag(String tagName, XMLAttrVector attributes) {
        beginBeginTag(tagName);
        if (attributes != null) {
            attributes.display(out, indent);
        }
        endBeginTag(tagName);
    }

    public void beginBeginTag(String tagName) {
        if (inTag) {
            // complete the parent's start tag
            if (compact) {
                out.print(">");
            } else {
                out.println(">");
            }
            inTag = false;
        }
        displayIndent(out, indent);
        out.print("<");
        out.print(tagName);
    }

    public void endBeginTag(String tagName) {
        if (glob) {
            inTag = true;
        } else if (compact) {
            out.print(">");
        } else {
            out.println(">");
        }
        out.flush();
        tagStack.push(tagName);
        indent++;
        tagsWritten++;
    }

    /** Writes an attribute. */
    public void attribute(String name, String value) {
        printAtt(out, name, value);
    }

    /** If we are currently inside the start tag, finishes it off. */
    public void beginNode() {
        if (inTag) {
            // complete the parent's start tag
            if (compact) {
                out.print(">");
            } else {
                out.println(">");
            }
            inTag = false;
        }
    }

    /**
     * Completes a tag. This outputs the end tag corresponding to the last exposed beginTag. The tag
     * name must match the name of the corresponding beginTag.
     *
     * @param tagName the name of the end tag to write.
     */
    public void endTag(String tagName) {
        // Check that the end tag matches the corresponding start tag
        String x = tagStack.pop();
        assert x.equals(tagName);

        // Lower the indent and display the end tag
        indent--;
        if (inTag) {
            // we're still in the start tag -- this element had no children
            if (compact) {
                out.print("/>");
            } else {
                out.println("/>");
            }
            inTag = false;
        } else {
            displayIndent(out, indent);
            out.print("</");
            out.print(tagName);
            if (compact) {
                out.print(">");
            } else {
                out.println(">");
            }
        }
        out.flush();
    }

    /**
     * Writes an empty tag to the stream. An empty tag is one with no tags inside it, although it
     * may still have attributes.
     *
     * @param tagName the name of the empty tag.
     * @param attributes an XMLAttrVector containing the attributes to include in the tag.
     */
    public void emptyTag(String tagName, XMLAttrVector attributes) {
        if (inTag) {
            // complete the parent's start tag
            if (compact) {
                out.print(">");
            } else {
                out.println(">");
            }
            inTag = false;
        }
        displayIndent(out, indent);
        out.print("<");
        out.print(tagName);
        if (attributes != null) {
            out.print(" ");
            attributes.display(out, indent);
        }

        if (compact) {
            out.print("/>");
        } else {
            out.println("/>");
        }
        out.flush();
        tagsWritten++;
    }

    /**
     * Writes a CDATA section. Such sections always appear on their own line. The nature in which
     * the CDATA section is written depends on the actual string content with respect to these
     * special characters/sequences:
     *
     * <ul>
     *   <li><code>&amp;</code>
     *   <li><code>&quot;</code>
     *   <li><code>'</code>
     *   <li><code>&lt;</code>
     *   <li><code>&gt;</code>
     * </ul>
     *
     * <p>Additionally, the sequence <code>]]&gt;</code> is special.
     *
     * <ul>
     *   <li>Content containing no special characters will be left as-is.
     *   <li>Content containing one or more special characters but not the sequence <code>]]&gt;
     *       </code> will be enclosed in a CDATA section.
     *   <li>Content containing special characters AND at least one <code>]]&gt;</code> sequence
     *       will be left as-is but have all of its special characters encoded as entities.
     * </ul>
     *
     * <p>These special treatment rules are required to allow cdata sections to contain XML strings
     * which may themselves contain cdata sections. Traditional CDATA sections <b>do not nest</b>.
     */
    public void cdata(String data) {
        cdata(data, false);
    }

    /**
     * Writes a CDATA section (as {@link #cdata(String)}).
     *
     * @param data string to write
     * @param quote if true, quote in a <code>&lt;![CDATA[</code> ... <code>]]&gt;</code> regardless
     *     of the content of <code>data</code>; if false, quote only if the content needs it
     */
    public void cdata(String data, boolean quote) {
        if (inTag) {
            // complete the parent's start tag
            if (compact) {
                out.print(">");
            } else {
                out.println(">");
            }
            inTag = false;
        }
        if (data == null) {
            data = "";
        }
        boolean specials = false;
        boolean cdataEnd = false;

        // Scan the string for special characters
        // If special characters are found, scan the string for ']]>'
        if (stringHasXMLSpecials(data)) {
            specials = true;
            if (data.contains("]]>")) {
                cdataEnd = true;
            }
        }

        // Display the result
        displayIndent(out, indent);
        if (quote || alwaysQuoteCData) {
            out.print("<![CDATA[");
            out.print(data);
            out.println("]]>");
        } else if (!specials) {
            out.println(data);
        } else {
            stringEncodeXML(data, out);
            out.println();
        }
        out.flush();
        tagsWritten++;
    }

    /** Writes a String tag; a tag containing nothing but a CDATA section. */
    public void stringTag(String name, String data) {
        beginTag(name, null);
        cdata(data);
        endTag(name);
    }

    /** Writes content. */
    public void content(String content) {
        // This method previously used a LineNumberReader, but that class is
        // susceptible to a form of DoS attack. It uses lots of memory and CPU if a
        // malicious client gives it input with very long lines.
        if (content != null) {
            indent++;
            final char[] chars = content.toCharArray();
            int prev = 0;
            for (int i = 0; i < chars.length; i++) {
                if (chars[i] == '\n'
                        || chars[i] == '\r' && i + 1 < chars.length && chars[i + 1] == '\n') {
                    displayIndent(out, indent);
                    out.println(content.substring(prev, i));
                    if (chars[i] == '\r') {
                        ++i;
                    }
                    prev = i + 1;
                }
            }
            displayIndent(out, indent);
            out.println(content.substring(prev, chars.length));
            indent--;
            out.flush();
        }
        tagsWritten++;
    }

    /** Write header. Use default version 1.0. */
    public void header() {
        out.println("<?xml version=\"1.0\" ?>");
        out.flush();
        tagsWritten++;
    }

    /** Write header, take version as input. */
    public void header(String version) {
        out.print("<?xml version=\"");
        out.print(version);
        out.println("\" ?>");
        out.flush();
        tagsWritten++;
    }

    /**
     * Get the total number of tags written.
     *
     * @return the total number of tags written to the XML stream.
     */
    public int numTagsWritten() {
        return tagsWritten;
    }

    /** Print an XML attribute name and value for string val. */
    private static void printAtt(PrintWriter pw, String name, String val) {
        if (val != null /* && !val.equals("") */) {
            pw.print(" ");
            pw.print(name);
            pw.print("=\"");
            pw.print(escapeForQuoting(val));
            pw.print("\"");
        }
    }

    /**
     * Encode a String for XML output, displaying it to a PrintWriter. The String to be encoded is
     * displayed, except that special characters are converted into entities.
     *
     * @param input a String to convert.
     * @param out a PrintWriter to which to write the results.
     */
    private static void stringEncodeXML(String input, PrintWriter out) {
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (c) {
                case '<':
                case '>':
                case '"':
                case '\'':
                case '&':
                case '\t':
                case '\n':
                case '\r':
                    out.print("&#" + (int) c + ";");
                    break;
                default:
                    out.print(c);
            }
        }
    }

    private static String escapeForQuoting(String val) {
        return StringEscaper.XML_NUMERIC_ESCAPER.escapeString(val);
    }

    /**
     * Returns whether a string contains any XML special characters.
     *
     * <p>If this function returns true, the string will need to be encoded either using the
     * stringEncodeXML function above or using a CDATA section. Note that MSXML has a nasty bug
     * whereby whitespace characters outside of a CDATA section are lost when parsing. To avoid
     * hitting this bug, this method treats many whitespace characters as "special".
     *
     * @param input the String to scan for XML special characters.
     * @return true if the String contains any such characters.
     */
    private static boolean stringHasXMLSpecials(String input) {
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (c) {
                case '<':
                case '>':
                case '"':
                case '\'':
                case '&':
                case '\t':
                case '\n':
                case '\r':
                    return true;
            }
        }
        return false;
    }

    /**
     * Utility for replacing special characters with escape sequences in strings.
     *
     * <p>A StringEscaper starts out as an identity transform in the "mutable" state. Call {@link
     * #defineEscape} as many times as necessary to set up mappings, and then call {@link
     * #makeImmutable} before actually applying the defined transform. Or, use one of the global
     * mappings pre-defined here.
     */
    static class StringEscaper implements Cloneable {
        private List<String> translationVector;
        private String[] translationTable;

        public static final StringEscaper XML_ESCAPER;
        public static final StringEscaper XML_NUMERIC_ESCAPER;
        public static final StringEscaper HTML_ESCAPER;
        public static final StringEscaper URL_ARG_ESCAPER;
        public static final StringEscaper URL_ESCAPER;

        /** Identity transform. */
        StringEscaper() {
            translationVector = new ArrayList<>();
        }

        /** Map character "from" to escape sequence "to". */
        public void defineEscape(char from, String to) {
            int i = (int) from;
            if (i >= translationVector.size()) {
                // Extend list by adding the requisite number of nulls.
                final int count = i + 1 - translationVector.size();
                translationVector.addAll(Collections.nCopies(count, null));
            }
            translationVector.set(i, to);
        }

        /**
         * Call this before attempting to escape strings; after this, defineEscape may not be called
         * again.
         */
        public void makeImmutable() {
            translationTable = translationVector.toArray(new String[0]);
            translationVector = null;
        }

        /** Apply an immutable transformation to the given string. */
        public String escapeString(String s) {
            StringBuilder sb = null;
            int n = s.length();
            for (int i = 0; i < n; i++) {
                char c = s.charAt(i);
                String escape;
                // codes >= 128 (e.g. Euro sign) are always escaped
                if (c > 127) {
                    escape = "&#" + Integer.toString(c) + ";";
                } else if (c >= translationTable.length) {
                    escape = null;
                } else {
                    escape = translationTable[c];
                }
                if (escape == null) {
                    if (sb != null) {
                        sb.append(c);
                    }
                } else {
                    if (sb == null) {
                        sb = new StringBuilder(n * 2);
                        sb.append(s, 0, i);
                    }
                    sb.append(escape);
                }
            }

            if (sb == null) {
                return s;
            } else {
                return sb.toString();
            }
        }

        protected StringEscaper clone() {
            StringEscaper clone = new StringEscaper();
            if (translationVector != null) {
                clone.translationVector = new ArrayList<>(translationVector);
            }
            if (translationTable != null) {
                clone.translationTable = translationTable.clone();
            }
            return clone;
        }

        /** Create a mutable escaper from an existing escaper, which may already be immutable. */
        public StringEscaper getMutableClone() {
            StringEscaper clone = clone();
            if (clone.translationVector == null) {
                clone.translationVector = Arrays.asList(clone.translationTable);
                clone.translationTable = null;
            }
            return clone;
        }

        static {
            HTML_ESCAPER = new StringEscaper();
            HTML_ESCAPER.defineEscape('&', "&amp;");
            HTML_ESCAPER.defineEscape('"', "&quot;");
            //      htmlEscaper.defineEscape('\'',"&apos;");
            HTML_ESCAPER.defineEscape('\'', "&#39;");
            HTML_ESCAPER.defineEscape('<', "&lt;");
            HTML_ESCAPER.defineEscape('>', "&gt;");

            XML_NUMERIC_ESCAPER = new StringEscaper();
            XML_NUMERIC_ESCAPER.defineEscape('&', "&#38;");
            XML_NUMERIC_ESCAPER.defineEscape('"', "&#34;");
            XML_NUMERIC_ESCAPER.defineEscape('\'', "&#39;");
            XML_NUMERIC_ESCAPER.defineEscape('<', "&#60;");
            XML_NUMERIC_ESCAPER.defineEscape('>', "&#62;");

            URL_ARG_ESCAPER = new StringEscaper();
            URL_ARG_ESCAPER.defineEscape('?', "%3f");
            URL_ARG_ESCAPER.defineEscape('&', "%26");
            URL_ESCAPER = URL_ARG_ESCAPER.getMutableClone();
            URL_ESCAPER.defineEscape('%', "%%");
            URL_ESCAPER.defineEscape('"', "%22");
            URL_ESCAPER.defineEscape('\r', "+");
            URL_ESCAPER.defineEscape('\n', "+");
            URL_ESCAPER.defineEscape(' ', "+");
            URL_ESCAPER.defineEscape('#', "%23");

            HTML_ESCAPER.makeImmutable();
            XML_ESCAPER = HTML_ESCAPER;
            XML_NUMERIC_ESCAPER.makeImmutable();
            URL_ARG_ESCAPER.makeImmutable();
            URL_ESCAPER.makeImmutable();
        }
    }

    /** List of attribute names and values. */
    static class XMLAttrVector {
        public void display(PrintWriter out, int indent) {
            throw new UnsupportedOperationException();
        }
    }
}

// End XmlOutput.java
