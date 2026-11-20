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
import org.apache.flink.table.runtime.functions.XmlVariantNames.SchemaPrefix;
import org.apache.flink.types.variant.Variant;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;

import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.runtime.functions.XmlVariantNames.ATTRIBUTE_PREFIX;
import static org.apache.flink.table.runtime.functions.XmlVariantNames.ORDER_KEY;
import static org.apache.flink.table.runtime.functions.XmlVariantNames.TEXT_KEY;
import static org.apache.flink.table.runtime.functions.XmlVariantNames.XSI_NIL;
import static org.apache.flink.table.runtime.functions.XmlVariantNames.XSI_TYPE;
import static org.apache.flink.table.runtime.functions.XmlVariantNames.parseXmlSchemaBoolean;
import static org.apache.flink.types.variant.BinaryVariantUtil.TIMESTAMP_FORMATTER;
import static org.apache.flink.types.variant.BinaryVariantUtil.TIMESTAMP_LTZ_FORMATTER;
import static org.apache.flink.types.variant.BinaryVariantUtil.TIME_FORMATTER;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Writes a {@link Variant} as an XML string for {@code XML_STRING}, the inverse of {@link
 * XmlToVariantParser}. The mapping, and the variants without an XML representation that fail with
 * an {@link IllegalArgumentException}, are described in the "VARIANT to XML String" section of the
 * documentation of the built-in functions.
 */
@Internal
public final class VariantToXmlWriter {

    private static final Set<Variant.Type> INTEGER_TYPES =
            EnumSet.of(
                    Variant.Type.TINYINT,
                    Variant.Type.SMALLINT,
                    Variant.Type.INT,
                    Variant.Type.BIGINT);

    // Whether a char below 0x40 is valid in XML 1.0 and written as is, in a text and in an
    // attribute value. Chars from 0x40 up to the surrogates are all written as is.
    private static final boolean[] TEXT_PLAIN_CHARS = plainChars(false);
    private static final boolean[] ATTRIBUTE_PLAIN_CHARS = plainChars(true);

    // The DOM document that names are checked with. It isn't thread-safe, so each thread has its
    // own.
    private static final ThreadLocal<Document> NAME_DOCUMENT =
            ThreadLocal.withInitial(VariantToXmlWriter::newDocument);

    // The document that is written, and the schema prefixes that it uses.
    private final StringBuilder output = new StringBuilder();

    // The elements whose content is still being written, the innermost first. The content is
    // written from here instead of from the element, so that the depth of the variant doesn't add
    // to the depth of the call stack.
    private final Deque<OpenElement> openElements = new ArrayDeque<>();
    private final Set<SchemaPrefix> usedSchemaPrefixes = EnumSet.noneOf(SchemaPrefix.class);

    private VariantToXmlWriter() {}

    public static String write(Variant variant) {
        final String rootName = rootName(variant);
        final Variant root = variant.getField(rootName);
        throwIfRootIsArray(rootName, root);
        return new VariantToXmlWriter().writeDocument(rootName, root);
    }

    /** Returns the name of the root element, the single field of the variant. */
    private static String rootName(Variant variant) {
        checkArgument(
                variant.isObject(),
                "The variant must be an object with a single field for the root element, but it "
                        + "is of type %s.",
                variant.getType());
        final List<String> fieldNames = variant.getFieldNames();
        checkArgument(
                fieldNames.size() == 1,
                "The variant must be an object with a single field for the root element, but it "
                        + "has %s fields.",
                fieldNames.size());
        return fieldNames.get(0);
    }

    private static void throwIfRootIsArray(String rootName, Variant root) {
        if (root.isArray()) {
            throw new IllegalArgumentException(
                    String.format(
                            "The root element '%s' can't be an array, since a document has only "
                                    + "one root element.",
                            rootName));
        }
    }

    // --------------------------------------------------------------------------------------------

    private String writeDocument(String rootName, Variant root) {
        writeElement(rootName, root);
        while (!openElements.isEmpty()) {
            writeNextContent(openElements.peek());
        }
        // inserting shifts the whole document, so it is skipped when there is nothing to insert
        final String declarations = schemaPrefixDeclarations(root);
        if (!declarations.isEmpty()) {
            insertIntoRootTag(rootName, declarations);
        }
        return output.toString();
    }

    /**
     * Returns the declarations of the schema prefixes that the document uses, without the ones that
     * the root element declares itself. A declaration on another element only applies within that
     * element, so the root element needs its own.
     */
    private String schemaPrefixDeclarations(Variant root) {
        final StringBuilder declarations = new StringBuilder();
        for (SchemaPrefix schemaPrefix : usedSchemaPrefixes) {
            if (!declaresOnRoot(root, schemaPrefix)) {
                declarations.append(
                        String.format(
                                " %s=\"%s\"", schemaPrefix.declaration, schemaPrefix.namespace));
            }
        }
        return declarations.toString();
    }

    private static boolean declaresOnRoot(Variant root, SchemaPrefix schemaPrefix) {
        return root.isObject()
                && root.getField(ATTRIBUTE_PREFIX + schemaPrefix.declaration) != null;
    }

    /**
     * Inserts the attributes after the name of the root element, which the document starts with.
     * They are inserted at the end, since only then it is known which prefixes the document uses.
     */
    private void insertIntoRootTag(String rootName, String attributes) {
        output.insert("<".length() + rootName.length(), attributes);
    }

    // --------------------------------------------------------------------------------------------
    // Elements
    // --------------------------------------------------------------------------------------------

    /** Writes a single item of a key, which isn't an array. */
    private void writeElement(String name, Variant value) {
        if (value.isNull()) {
            writeNilElement(name);
        } else if (value.isObject()) {
            writeObjectElement(name, value);
        } else {
            writeScalarElement(name, value);
        }
    }

    /** A variant {@code NULL} is nil, since an empty element is an empty string. */
    private void writeNilElement(String name) {
        openStartTag(name);
        writeXsiNil();
        closeEmptyElement();
    }

    private void writeScalarElement(String name, Variant scalar) {
        openStartTag(name);
        writeXsiType(scalar);
        closeStartTag();
        writeText(name, scalar);
        writeEndTag(name);
    }

    /**
     * Writes an object as an element with the {@code @} fields as attributes, the {@code $} field
     * as text, and the other fields as child elements.
     */
    private void writeObjectElement(String name, Variant element) {
        final ElementKeys keys = new ElementKeys(element);
        final List<ContentItem> content = content(name, element, keys.contentKeys);
        if (hasNullText(content)) {
            writeNilElementWithAttributes(name, element, keys.attributeKeys);
        } else {
            writeElementWithContent(name, element, keys.attributeKeys, content);
        }
    }

    /** Like in {@code PARSE_XML}, a nil element with attributes has a null text and no content. */
    private static boolean hasNullText(List<ContentItem> content) {
        final @Nullable Variant text = onlyText(content);
        return text != null && text.isNull();
    }

    private void writeNilElementWithAttributes(
            String name, Variant element, List<String> attributeKeys) {
        checkNoAttributeField(name, element, XSI_NIL, "a null text");
        openStartTag(name);
        writeFields(name, element, attributeKeys);
        writeXsiNil();
        closeEmptyElement();
    }

    private void writeElementWithContent(
            String name, Variant element, List<String> attributeKeys, List<ContentItem> content) {
        checkIsNotNil(name, element, content);
        openStartTag(name);
        writeFields(name, element, attributeKeys);
        writeXsiTypeOfText(name, element, content);
        closeStartTag();
        openElements.push(new OpenElement(name, content));
    }

    /**
     * Writes the next item of the content, which opens another element if it is an object, or the
     * end tag after the last item.
     */
    private void writeNextContent(OpenElement element) {
        if (element.next < element.content.size()) {
            final ContentItem item = element.content.get(element.next++);
            writeField(element.name, item.key, item.value);
        } else {
            openElements.pop();
            writeEndTag(element.name);
        }
    }

    /** Like in {@code PARSE_XML}, only the text of an element without child elements is typed. */
    private void writeXsiTypeOfText(String name, Variant element, List<ContentItem> content) {
        final @Nullable Variant text = onlyText(content);
        if (text != null && xsiType(text) != null) {
            checkNoAttributeField(name, element, XSI_TYPE, "a typed text");
            writeXsiType(text);
        }
    }

    /** {@code PARSE_XML} drops the content of a nil element, so it can't have content. */
    private static void checkIsNotNil(
            String elementName, Variant element, List<ContentItem> content) {
        final @Nullable Variant xsiNil = element.getField(ATTRIBUTE_PREFIX + XSI_NIL);
        checkArgument(
                content.isEmpty() || xsiNil == null || !isTrue(xsiNil),
                "The element '%s' has an '%s%s' field that is true and content, but an element "
                        + "with %s can't have content.",
                elementName,
                ATTRIBUTE_PREFIX,
                XSI_NIL,
                XSI_NIL);
    }

    /** An xsi:nil attribute is true for the values that {@code PARSE_XML} reads as true. */
    private static boolean isTrue(Variant xsiNil) {
        return isNonNullScalar(xsiNil)
                && Boolean.TRUE.equals(parseXmlSchemaBoolean(lexicalForm(xsiNil).trim()));
    }

    /** An element can't have an {@code @} field for an attribute that the writer adds as well. */
    private static void checkNoAttributeField(
            String elementName, Variant element, String attribute, String reason) {
        checkArgument(
                element.getField(ATTRIBUTE_PREFIX + attribute) == null,
                "The element '%s' has an '%s%s' field and %s, but an element can have only one %s "
                        + "attribute.",
                elementName,
                ATTRIBUTE_PREFIX,
                attribute,
                reason,
                attribute);
    }

    // --------------------------------------------------------------------------------------------
    // Fields
    // --------------------------------------------------------------------------------------------

    private void writeFields(String elementName, Variant element, List<String> keys) {
        for (String key : keys) {
            writeField(elementName, key, element.getField(key));
        }
    }

    /**
     * Writes a field of an element by its key: an {@code @} field as an attribute, the {@code $}
     * field as text, and any other field as a child element.
     */
    private void writeField(String elementName, String key, Variant value) {
        if (key.startsWith(ATTRIBUTE_PREFIX)) {
            writeAttributeField(elementName, key.substring(ATTRIBUTE_PREFIX.length()), value);
        } else if (key.equals(TEXT_KEY)) {
            writeText(elementName, value);
        } else {
            writeElement(key, value);
        }
    }

    private void writeAttributeField(String elementName, String name, Variant value) {
        checkAttributeName(name);
        checkIsAttributeValue(elementName, name, value);
        final String text = lexicalForm(value);
        openAttribute(name, text);
        final int nonXmlCharIndex = appendEscaped(text, true);
        output.append('"');
        if (nonXmlCharIndex >= 0) {
            throw nonXmlChar(
                    String.format("The attribute '%s' of the element '%s'", name, elementName),
                    text,
                    nonXmlCharIndex);
        }
    }

    private static void checkIsAttributeValue(String elementName, String name, Variant value) {
        checkArgument(
                isNonNullScalar(value),
                "The attribute '%s' of the element '%s' can't be of type %s.",
                name,
                elementName,
                value.getType());
    }

    private void writeXsiNil() {
        writeAttribute(XSI_NIL, "true");
    }

    private void writeXsiType(Variant scalar) {
        final @Nullable String xsiType = xsiType(scalar);
        if (xsiType != null) {
            writeAttribute(XSI_TYPE, SchemaPrefix.XS.prefix + xsiType);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Writing the markup
    // --------------------------------------------------------------------------------------------

    private void openStartTag(String name) {
        checkElementName(name);
        noteSchemaPrefix(name);
        output.append('<').append(name);
    }

    private void closeStartTag() {
        output.append('>');
    }

    private void closeEmptyElement() {
        output.append("/>");
    }

    private void writeEndTag(String name) {
        output.append("</").append(name).append('>');
    }

    /** Writes xsi:nil or xsi:type, whose values the writer sets and which need no escaping. */
    private void writeAttribute(String name, String value) {
        openAttribute(name, value);
        output.append(value).append('"');
    }

    /** The name is checked by the caller, since the writer adds xsi:nil and xsi:type itself. */
    private void openAttribute(String name, String value) {
        noteSchemaPrefix(name);
        if (name.equals(XSI_TYPE)) {
            // The value of an xsi:type is a name with a prefix as well, e.g. xs:int.
            noteSchemaPrefix(value);
        }
        output.append(' ').append(name).append("=\"");
    }

    private void writeText(String elementName, Variant value) {
        checkIsText(elementName, value);
        final String text = lexicalForm(value);
        final int nonXmlCharIndex = appendEscaped(text, false);
        if (nonXmlCharIndex >= 0) {
            throw nonXmlChar(
                    String.format("The text of the element '%s'", elementName),
                    text,
                    nonXmlCharIndex);
        }
    }

    /**
     * Appends the text escaped, and checks in the same pass for chars that XML 1.0 can't represent,
     * not even as character references. Returns the index of the first such char, or -1 if there is
     * none. Chars that aren't escaped are appended in runs.
     */
    private int appendEscaped(String text, boolean isAttribute) {
        final boolean[] plainChars = isAttribute ? ATTRIBUTE_PLAIN_CHARS : TEXT_PLAIN_CHARS;
        // Grow once up front. The extra room is a guess for escapes and the markup after the text,
        // which would otherwise grow the output again.
        final int length = text.length();
        output.ensureCapacity(output.length() + length + length / 8 + 16);
        int start = 0;
        for (int i = 0; i < length; i++) {
            final char c = text.charAt(i);
            if (c < plainChars.length ? plainChars[c] : c < 0xD800) {
                continue;
            }
            if (c < plainChars.length) {
                final String escape = escape(c, isAttribute);
                if (escape == null) {
                    // a control char
                    return i;
                }
                output.append(text, start, i).append(escape);
                start = i + 1;
            } else if (Character.isHighSurrogate(c)
                    && i + 1 < length
                    && Character.isLowSurrogate(text.charAt(i + 1))) {
                // a surrogate pair encodes a char from U+10000 up, which is allowed
                i++;
            } else if (c < 0xE000 || c > 0xFFFD) {
                // a lone surrogate, U+FFFE or U+FFFF
                return i;
            }
        }
        if (start == 0) {
            output.append(text);
        } else {
            output.append(text, start, length);
        }
        return -1;
    }

    private static IllegalArgumentException nonXmlChar(String location, String text, int index) {
        return new IllegalArgumentException(
                String.format(
                        "%s has the character U+%04X, which XML 1.0 doesn't allow.",
                        location, text.codePointAt(index)));
    }

    private static boolean[] plainChars(boolean isAttribute) {
        final boolean[] plainChars = new boolean[0x40];
        for (char c = 0; c < plainChars.length; c++) {
            plainChars[c] =
                    (c >= 0x20 || isAllowedControlChar(c)) && escape(c, isAttribute) == null;
        }
        return plainChars;
    }

    /** The control chars that XML 1.0 allows. */
    private static boolean isAllowedControlChar(char c) {
        return c == '\t' || c == '\n' || c == '\r';
    }

    // Markup characters are escaped. A parser reads a carriage return as a line feed, and a tab or
    // a line break in an attribute value as a space, so these are written as character references.
    @Nullable
    private static String escape(char c, boolean isAttribute) {
        switch (c) {
            case '&':
                return "&amp;";
            case '<':
                return "&lt;";
            case '>':
                return "&gt;";
            case '\r':
                return "&#13;";
            case '"':
                return isAttribute ? "&quot;" : null;
            case '\t':
                return isAttribute ? "&#9;" : null;
            case '\n':
                return isAttribute ? "&#10;" : null;
            default:
                return null;
        }
    }

    private void noteSchemaPrefix(String name) {
        for (SchemaPrefix schemaPrefix : SchemaPrefix.ALL) {
            if (name.startsWith(schemaPrefix.prefix)) {
                usedSchemaPrefixes.add(schemaPrefix);
            }
        }
    }

    // A key can't add markup, so it has to be a valid name.

    private static void checkElementName(String name) {
        checkArgument(isName(name), "'%s' is not a valid XML element name.", name);
    }

    private static void checkAttributeName(String name) {
        checkArgument(isName(name), "'%s' is not a valid XML attribute name.", name);
    }

    /**
     * Whether {@code PARSE_XML} accepts the name, without checking its namespace prefix. The DOM of
     * the JDK accepts the same names as the XML parser of the JDK that {@code PARSE_XML} uses.
     */
    private static boolean isName(String name) {
        try {
            NAME_DOCUMENT.get().createElement(name);
            return true;
        } catch (DOMException e) {
            return false;
        }
    }

    private static Document newDocument() {
        try {
            return DocumentBuilderFactory.newDefaultInstance().newDocumentBuilder().newDocument();
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * A text has to be a scalar. A {@code NULL} text is written as nil, which is only possible if
     * it is the only content of its element.
     */
    private static void checkIsText(String elementName, Variant text) {
        checkArgument(
                !text.isNull(),
                "The element '%s' has a null text and other content, but an element with %s can't "
                        + "have content.",
                elementName,
                XSI_NIL);
        checkArgument(
                isNonNullScalar(text),
                "The text of the element '%s' can't be of type %s.",
                elementName,
                text.getType());
    }

    // --------------------------------------------------------------------------------------------
    // Content
    // --------------------------------------------------------------------------------------------

    /**
     * Returns the child elements and texts of an element, in the order of its {@code #} field.
     * Items without a position follow in the order of their keys.
     */
    private static List<ContentItem> content(
            String elementName, Variant element, List<String> contentKeys) {
        // For example, {"a":{"#":{"$":[0,2],"c":1},"$":["x","y"],"c":"z"}} is <a>x<c>z</c>y</a>,
        // since the texts x and y are at the positions 0 and 2, and the element c at 1.
        final Map<String, List<Long>> positionsByKey = positionsByKey(elementName, element);
        final List<ContentItem> content = new ArrayList<>();
        for (final String key : contentKeys) {
            final List<Variant> items = items(element.getField(key));
            checkNoNestedArray(elementName, key, items);
            final @Nullable List<Long> positions = positionsByKey.get(key);
            checkPositionCount(elementName, key, positions, items.size());
            for (int i = 0; i < items.size(); i++) {
                final @Nullable Long position = positions == null ? null : positions.get(i);
                content.add(new ContentItem(key, items.get(i), position));
            }
        }
        content.sort(ContentItem.BY_POSITION);
        checkPositionsAreDistinct(elementName, content);
        return content;
    }

    /** Returns the text if it is the only content of an element, or else null. */
    private static @Nullable Variant onlyText(List<ContentItem> content) {
        return content.size() == 1 && content.get(0).isText() ? content.get(0).value : null;
    }

    /**
     * Returns the positions in the {@code #} field by key, or no positions if the element has no
     * {@code #} field. The positions of a key are stored like its items: a single position, or an
     * array of positions.
     */
    private static Map<String, List<Long>> positionsByKey(String elementName, Variant element) {
        final @Nullable Variant order = element.getField(ORDER_KEY);
        if (order == null) {
            return Collections.emptyMap();
        }
        checkArgument(
                order.isObject(),
                "The '%s' field of the element '%s' must be an object, but it is of type %s.",
                ORDER_KEY,
                elementName,
                order.getType());
        final Map<String, List<Long>> positionsByKey = new HashMap<>();
        for (String key : order.getFieldNames()) {
            final List<Long> positions = new ArrayList<>();
            for (Variant position : items(order.getField(key))) {
                positions.add(position(elementName, key, position));
            }
            positionsByKey.put(key, positions);
        }
        return positionsByKey;
    }

    private static long position(String elementName, String key, Variant position) {
        checkArgument(
                INTEGER_TYPES.contains(position.getType()),
                "The '%s' field of the element '%s' has a position of type %s for '%s', but a "
                        + "position must be an integer.",
                ORDER_KEY,
                elementName,
                position.getType(),
                key);
        // For the integer types, get() returns a Byte, Short, Integer, or Long.
        return ((Number) position.get()).longValue();
    }

    /** Returns the items of a value: the elements of an array, or else the value itself. */
    private static List<Variant> items(Variant value) {
        if (!value.isArray()) {
            return Collections.singletonList(value);
        }
        final List<Variant> items = new ArrayList<>(value.getArraySize());
        for (int i = 0; i < value.getArraySize(); i++) {
            items.add(value.getElement(i));
        }
        return items;
    }

    private static void checkNoNestedArray(String elementName, String key, List<Variant> items) {
        checkArgument(
                items.stream().noneMatch(Variant::isArray),
                "The field '%s' of the element '%s' has an array nested in an array, which XML "
                        + "can't represent.",
                key,
                elementName);
    }

    /** A key with positions in the {@code #} field has one position for each of its items. */
    private static void checkPositionCount(
            String elementName, String key, @Nullable List<Long> positions, int itemCount) {
        if (positions != null) {
            checkArgument(
                    positions.size() == itemCount,
                    "The number of positions for '%s' in the '%s' field of the element '%s' is "
                            + "%s, but the number of items is %s.",
                    key,
                    ORDER_KEY,
                    elementName,
                    positions.size(),
                    itemCount);
        }
    }

    /** The content is sorted by position, so a position that is used twice has equal neighbors. */
    private static void checkPositionsAreDistinct(String elementName, List<ContentItem> content) {
        for (int i = 1; i < content.size(); i++) {
            final @Nullable Long position = content.get(i).position;
            checkArgument(
                    position == null || !position.equals(content.get(i - 1).position),
                    "The '%s' field of the element '%s' has the position %s more than once.",
                    ORDER_KEY,
                    elementName,
                    position);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Scalars
    // --------------------------------------------------------------------------------------------

    private static boolean isNonNullScalar(Variant value) {
        return value.isPrimitive() && !value.isNull();
    }

    /**
     * Returns the XML Schema type of a value, or null if it has none: a string or a UUID, which XML
     * Schema has no type for, and an object, an array, or {@code NULL}.
     */
    private static @Nullable String xsiType(Variant value) {
        switch (value.getType()) {
            case BOOLEAN:
                return "boolean";
            case TINYINT:
                return "byte";
            case SMALLINT:
                return "short";
            case INT:
                return "int";
            case BIGINT:
                return "long";
            case DECIMAL:
                return "decimal";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case DATE:
                return "date";
            case TIME:
                return "time";
            case TIMESTAMP:
            case TIMESTAMP_NS:
            case TIMESTAMP_LTZ:
            case TIMESTAMP_LTZ_NS:
                return "dateTime";
            case BYTES:
                return "base64Binary";
            default:
                return null;
        }
    }

    /** Returns the text of a scalar, in the lexical form of its xsi:type. */
    private static String lexicalForm(Variant scalar) {
        switch (scalar.getType()) {
            case DECIMAL:
                return scalar.getDecimal().toPlainString();
            case FLOAT:
                return lexicalForm(scalar.getFloat());
            case DOUBLE:
                return lexicalForm(scalar.getDouble());
            case DATE:
                return lexicalForm(scalar.getDate());
            case TIME:
                return TIME_FORMATTER.format(scalar.getTime());
            case TIMESTAMP:
            case TIMESTAMP_NS:
                return lexicalForm(scalar.getDateTime());
            case TIMESTAMP_LTZ:
            case TIMESTAMP_LTZ_NS:
                return lexicalForm(scalar.getInstant());
            case BYTES:
                return Base64.getEncoder().encodeToString(scalar.getBytes());
            default:
                // A string, a boolean, an integer, or a UUID, whose string form is the lexical
                // form.
                return String.valueOf(scalar.get());
        }
    }

    // XML Schema writes infinity as INF instead of Infinity. NaN is written the same.

    private static String lexicalForm(float value) {
        return Float.isInfinite(value) ? infinity(value) : Float.toString(value);
    }

    private static String lexicalForm(double value) {
        return Double.isInfinite(value) ? infinity(value) : Double.toString(value);
    }

    private static String infinity(double value) {
        return value > 0 ? "INF" : "-INF";
    }

    // Java writes a year after 9999 with a + sign, which XML Schema doesn't allow.

    private static String lexicalForm(LocalDate date) {
        return withoutYearSign(date.toString());
    }

    private static String lexicalForm(LocalDateTime dateTime) {
        return withoutYearSign(TIMESTAMP_FORMATTER.format(dateTime));
    }

    /** An instant is written in UTC. */
    private static String lexicalForm(Instant instant) {
        return withoutYearSign(TIMESTAMP_LTZ_FORMATTER.format(instant.atOffset(ZoneOffset.UTC)));
    }

    private static String withoutYearSign(String dateOrDateTime) {
        return dateOrDateTime.startsWith("+") ? dateOrDateTime.substring(1) : dateOrDateTime;
    }

    // --------------------------------------------------------------------------------------------

    /** The keys of an object element: the {@code @} fields, and the fields of its content. */
    private static final class ElementKeys {

        final List<String> attributeKeys = new ArrayList<>();
        final List<String> contentKeys = new ArrayList<>();

        ElementKeys(Variant element) {
            for (String key : element.getFieldNames()) {
                if (key.startsWith(ATTRIBUTE_PREFIX)) {
                    attributeKeys.add(key);
                } else if (!key.equals(ORDER_KEY)) {
                    contentKeys.add(key);
                }
            }
        }
    }

    /** An element whose start tag is written, and the position of the next item of its content. */
    private static final class OpenElement {

        final String name;
        final List<ContentItem> content;
        int next;

        OpenElement(String name, List<ContentItem> content) {
            this.name = name;
            this.content = content;
        }
    }

    /** A child element or a text of an element, with the key that it is stored under. */
    private static final class ContentItem {

        // Items without a position follow the ones with one. The sort is stable, so they keep the
        // order of their keys.
        static final Comparator<ContentItem> BY_POSITION =
                Comparator.comparing(
                        item -> item.position, Comparator.nullsLast(Comparator.naturalOrder()));

        final String key;
        final Variant value;

        // The position in the # field, or null if it has none.
        final @Nullable Long position;

        ContentItem(String key, Variant value, @Nullable Long position) {
            this.key = key;
            this.value = value;
            this.position = position;
        }

        boolean isText() {
            return key.equals(TEXT_KEY);
        }
    }
}
