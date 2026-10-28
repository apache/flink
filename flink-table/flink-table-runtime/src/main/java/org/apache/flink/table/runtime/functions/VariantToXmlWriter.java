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
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import java.io.StringWriter;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.runtime.functions.XmlVariantNames.ATTRIBUTE_PREFIX;
import static org.apache.flink.table.runtime.functions.XmlVariantNames.ORDER_KEY;
import static org.apache.flink.table.runtime.functions.XmlVariantNames.TEXT_KEY;
import static org.apache.flink.table.runtime.functions.XmlVariantNames.XSI_NIL;
import static org.apache.flink.table.runtime.functions.XmlVariantNames.XSI_TYPE;
import static org.apache.flink.types.variant.BinaryVariantUtil.TIMESTAMP_FORMATTER;
import static org.apache.flink.types.variant.BinaryVariantUtil.TIMESTAMP_LTZ_FORMATTER;
import static org.apache.flink.types.variant.BinaryVariantUtil.TIME_FORMATTER;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Writes a {@link Variant} as an XML string for {@code XML_STRING}. It reverses the mapping of
 * {@link XmlToVariantParser}, so that parsing the XML string of a variant from {@code PARSE_XML}
 * gives the same variant. The mapping is described in the documentation of {@code XML_STRING}.
 *
 * <p>The content of an element is written in the order of its {@code #} field. Content without a
 * position in the {@code #} field follows in the order of its keys. A variant {@code NULL}, or a
 * {@code NULL} text, is written as {@code xsi:nil="true"}, and a scalar other than a string or a
 * UUID gets an {@code xsi:type}, e.g. {@code xsi:type="xs:int"}. If the document uses the xsi or xs
 * prefix, the root element declares it with the namespace of XML Schema, unless the variant
 * declares it on the root element itself.
 *
 * <p>The XML writer of the JDK writes the tags and escapes the text. It writes tabs and line breaks
 * in attribute values, and carriage returns in text, as they are, so a parser reads them back as
 * spaces and line feeds.
 *
 * <p>A variant fails with an {@link IllegalArgumentException} if it has no XML representation:
 *
 * <ul>
 *   <li>it isn't an object with a single field for the root element, or the root is an array,
 *   <li>a key isn't an XML name that {@code PARSE_XML} accepts,
 *   <li>a text or an attribute value has a character that XML 1.0 doesn't allow, e.g. U+0000,
 *   <li>an array is nested in an array,
 *   <li>an {@code @} field holds an object, an array, or a {@code NULL}, or a {@code $} field holds
 *       an object,
 *   <li>a {@code NULL} text isn't the only content of its element, or an element has an {@code
 *       @xsi:nil} field that is true and content,
 *   <li>a {@code #} field isn't an object, doesn't have one integer position for each item of a
 *       key, or has a position more than once,
 *   <li>an element would need two {@code xsi:type} or two {@code xsi:nil} attributes, e.g. an
 *       {@code @xsi:type} field and a typed text, or an {@code @xsi:nil} field and a {@code NULL}
 *       text.
 * </ul>
 */
@Internal
public final class VariantToXmlWriter {

    private static final Set<Variant.Type> INTEGER_TYPES =
            EnumSet.of(
                    Variant.Type.TINYINT,
                    Variant.Type.SMALLINT,
                    Variant.Type.INT,
                    Variant.Type.BIGINT);

    // The XML writer of the JDK, regardless of other StAX implementations on the classpath. Its
    // factory creates a new writer for each call and keeps no state of it, so it can be shared.
    private static final XMLOutputFactory OUTPUT_FACTORY = XMLOutputFactory.newDefaultFactory();

    // Any character outside of the Char production of XML 1.0, see
    // https://www.w3.org/TR/xml/#charsets.
    private static final Pattern NON_XML_CHAR =
            Pattern.compile("[^\t\n\r\u0020-\uD7FF\uE000-\uFFFD\\x{10000}-\\x{10FFFF}]");

    // The DOM document that names are checked with. It isn't thread-safe, so each thread has its
    // own.
    private static final ThreadLocal<Document> NAME_DOCUMENT =
            ThreadLocal.withInitial(VariantToXmlWriter::newDocument);

    // The document that is written, and the schema prefixes that it uses.
    private final StringWriter output = new StringWriter();
    private final XMLStreamWriter xml;
    private final Set<SchemaPrefix> usedSchemaPrefixes = EnumSet.noneOf(SchemaPrefix.class);

    private VariantToXmlWriter() {
        try {
            xml = OUTPUT_FACTORY.createXMLStreamWriter(output);
        } catch (XMLStreamException e) {
            // Only the message, since the location of the exception isn't serializable.
            throw new IllegalStateException(e.getMessage());
        }
    }

    public static String write(Variant variant) {
        final String rootName = rootName(variant);
        final Variant root = variant.getField(rootName);
        checkRootIsNotArray(rootName, root);
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

    private static void checkRootIsNotArray(String rootName, Variant root) {
        checkArgument(
                !root.isArray(),
                "The root element '%s' can't be an array, since a document has only one root "
                        + "element.",
                rootName);
    }

    // --------------------------------------------------------------------------------------------

    private String writeDocument(String rootName, Variant root) {
        try {
            writeElement(rootName, root);
            // Closes the tag of a nil root element, which an empty element leaves open.
            xml.writeEndDocument();
            xml.close();
        } catch (XMLStreamException e) {
            // Only the message, since the location of the exception isn't serializable.
            throw new IllegalStateException(e.getMessage());
        }
        insertIntoRootTag(rootName, schemaPrefixDeclarations(root));
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
     * Inserts the attributes after the name of the root element. They are inserted at the end,
     * since only then it is known which prefixes the document uses. The document starts with the
     * start tag of the root element, since it has no XML declaration.
     */
    private void insertIntoRootTag(String rootName, String attributes) {
        final StringBuffer document = output.getBuffer();
        final String rootTagStart = "<" + rootName;
        checkState(
                document.indexOf(rootTagStart) == 0,
                "The document doesn't start with %s.",
                rootTagStart);
        document.insert(rootTagStart.length(), attributes);
    }

    // --------------------------------------------------------------------------------------------
    // Elements
    // --------------------------------------------------------------------------------------------

    /** Writes a single item of a key, which isn't an array. */
    private void writeElement(String name, Variant value) throws XMLStreamException {
        if (value.isNull()) {
            writeNilElement(name);
        } else if (value.isObject()) {
            writeObjectElement(name, value);
        } else {
            writeScalarElement(name, value);
        }
    }

    /** A variant {@code NULL} is nil, since an empty element is an empty string. */
    private void writeNilElement(String name) throws XMLStreamException {
        writeEmptyElement(name);
        writeXsiNil();
    }

    private void writeScalarElement(String name, Variant scalar) throws XMLStreamException {
        writeStartElement(name);
        writeXsiType(scalar);
        writeText(name, scalar);
        writeEndElement();
    }

    /**
     * Writes an object as an element with the {@code @} fields as attributes, the {@code $} field
     * as text, and the other fields as child elements.
     */
    private void writeObjectElement(String name, Variant element) throws XMLStreamException {
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
            String name, Variant element, List<String> attributeKeys) throws XMLStreamException {
        checkNoAttributeField(name, element, XSI_NIL, "a null text");
        writeEmptyElement(name);
        writeFields(name, element, attributeKeys);
        writeXsiNil();
    }

    private void writeElementWithContent(
            String name, Variant element, List<String> attributeKeys, List<ContentItem> content)
            throws XMLStreamException {
        checkIsNotNil(name, element, content);
        writeStartElement(name);
        // Attributes come first, since the XML writer writes them into the start tag.
        writeFields(name, element, attributeKeys);
        writeXsiTypeOfText(name, element, content);
        for (ContentItem item : content) {
            writeField(name, item.key, item.value);
        }
        writeEndElement();
    }

    /** Like in {@code PARSE_XML}, only the text of an element without child elements is typed. */
    private void writeXsiTypeOfText(String name, Variant element, List<ContentItem> content)
            throws XMLStreamException {
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

    /** Like in {@code PARSE_XML}, xsi:nil is true for the values true and 1. */
    private static boolean isTrue(Variant xsiNil) {
        if (!isNonNullScalar(xsiNil)) {
            return false;
        }
        final String value = lexicalForm(xsiNil).trim();
        return value.equals("true") || value.equals("1");
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

    private void writeFields(String elementName, Variant element, List<String> keys)
            throws XMLStreamException {
        for (String key : keys) {
            writeField(elementName, key, element.getField(key));
        }
    }

    /**
     * Writes a field of an element by its key: an {@code @} field as an attribute, the {@code $}
     * field as text, and any other field as a child element.
     */
    private void writeField(String elementName, String key, Variant value)
            throws XMLStreamException {
        if (key.startsWith(ATTRIBUTE_PREFIX)) {
            writeAttributeField(elementName, key.substring(ATTRIBUTE_PREFIX.length()), value);
        } else if (key.equals(TEXT_KEY)) {
            writeText(elementName, value);
        } else {
            writeElement(key, value);
        }
    }

    private void writeAttributeField(String elementName, String name, Variant value)
            throws XMLStreamException {
        checkAttributeName(name);
        checkIsAttributeValue(elementName, name, value);
        checkXmlChars(elementName, name, value);
        writeAttribute(name, lexicalForm(value));
    }

    private static void checkIsAttributeValue(String elementName, String name, Variant value) {
        checkArgument(
                isNonNullScalar(value),
                "The attribute '%s' of the element '%s' can't be of type %s.",
                name,
                elementName,
                value.getType());
    }

    private void writeXsiNil() throws XMLStreamException {
        writeAttribute(XSI_NIL, "true");
    }

    private void writeXsiType(Variant scalar) throws XMLStreamException {
        final @Nullable String xsiType = xsiType(scalar);
        if (xsiType != null) {
            writeAttribute(XSI_TYPE, SchemaPrefix.XS.prefix + xsiType);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Writing to the XML writer, which writes the tags and escapes the text
    // --------------------------------------------------------------------------------------------

    private void writeStartElement(String name) throws XMLStreamException {
        checkElementName(name);
        noteSchemaPrefix(name);
        xml.writeStartElement(name);
    }

    private void writeEmptyElement(String name) throws XMLStreamException {
        checkElementName(name);
        noteSchemaPrefix(name);
        xml.writeEmptyElement(name);
    }

    private void writeEndElement() throws XMLStreamException {
        xml.writeEndElement();
    }

    /** The name is checked by the caller, since the writer adds xsi:nil and xsi:type itself. */
    private void writeAttribute(String name, String value) throws XMLStreamException {
        noteSchemaPrefix(name);
        if (name.equals(XSI_TYPE)) {
            // The value of an xsi:type is a name with a prefix as well, e.g. xs:int.
            noteSchemaPrefix(value);
        }
        xml.writeAttribute(name, value);
    }

    private void writeText(String elementName, Variant text) throws XMLStreamException {
        checkIsText(elementName, text);
        checkXmlChars(elementName, null, text);
        xml.writeCharacters(lexicalForm(text));
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
     * The XML writer writes every character as it is, but XML 1.0 can't represent some of them, not
     * even as character references. Only strings are checked, since the writer formats the other
     * scalars itself. The attribute name is null for a text.
     */
    private static void checkXmlChars(
            String elementName, @Nullable String attributeName, Variant scalar) {
        if (scalar.getType() != Variant.Type.STRING) {
            return;
        }
        final String text = scalar.getString();
        final Matcher matcher = NON_XML_CHAR.matcher(text);
        if (matcher.find()) {
            final String location =
                    attributeName == null
                            ? String.format("The text of the element '%s'", elementName)
                            : String.format(
                                    "The attribute '%s' of the element '%s'",
                                    attributeName, elementName);
            throw new IllegalArgumentException(
                    String.format(
                            "%s has the character U+%04X, which XML 1.0 doesn't allow.",
                            location, text.codePointAt(matcher.start())));
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
