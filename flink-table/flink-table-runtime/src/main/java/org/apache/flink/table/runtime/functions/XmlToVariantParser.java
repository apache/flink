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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder.FieldEntry;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantTypeException;

import javax.annotation.Nullable;
import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.types.variant.BinaryVariantInternalBuilder.toVariantDecimal;
import static org.apache.flink.types.variant.BinaryVariantUtil.SIZE_LIMIT;
import static org.apache.flink.types.variant.BinaryVariantUtil.microsSinceEpoch;
import static org.apache.flink.types.variant.BinaryVariantUtil.nanosSinceEpoch;

/**
 * Parses XML into a {@link Variant} for {@code PARSE_XML} and {@code TRY_PARSE_XML}. The mapping is
 * described in the documentation of the {@code VARIANT} data type.
 *
 * <p>Parsing has two steps. First, {@link #read} reads the document into a tree of {@link
 * XmlElement}s, which group the children of each element by name and apply {@code xsi:nil} and
 * {@code xsi:type}. Then, {@link #encode} writes the tree into a variant. The variant can't be
 * written while reading, because the occurrences of a repeated child become one array, and they
 * don't have to be next to each other in the document.
 *
 * <p>Instances are not thread-safe.
 */
@Internal
public final class XmlToVariantParser {

    private static final String ATTRIBUTE_PREFIX = "@";
    private static final String TEXT_KEY = "$";
    private static final String ORDER_KEY = "#";

    private static final String XSI_NIL = "xsi:nil";
    private static final String XSI_TYPE = "xsi:type";
    private static final String XSI_NAMESPACE_DECLARATION = "xmlns:xsi";

    private final XMLInputFactory inputFactory = createInputFactory();

    @VisibleForTesting
    Variant parse(String xml, boolean forceArray) {
        return encode(read(xml), forceArray);
    }

    /**
     * Reads the document into the tree of its root element. Calls on the same document can share
     * the tree, since {@link #encode} doesn't change it.
     */
    XmlElement read(String xml) {
        try {
            return readDocument(xml);
        } catch (XMLStreamException e) {
            // Only the message is kept, since the location of the exception is not serializable.
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    static Variant encode(XmlElement root, boolean forceArray) {
        return new VariantEncoder(forceArray).encodeDocument(root);
    }

    private static XMLInputFactory createInputFactory() {
        // The JDK implementation, regardless of other StAX implementations on the classpath.
        final XMLInputFactory factory = XMLInputFactory.newDefaultFactory();
        factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false);
        factory.setProperty(XMLInputFactory.IS_COALESCING, true);
        // Internal entities need DTD support. External entities are enabled only so that they
        // reach the resolver and fail, instead of being dropped silently. The JDK refuses all
        // protocols in case a load bypasses the resolver.
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, true);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, true);
        factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setXMLResolver(
                (publicId, systemId, baseUri, namespace) -> {
                    throw new XMLStreamException(
                            String.format(
                                    "External entities and DTDs are not supported: %s", systemId));
                });
        // Set here, so that they don't depend on the JDK version or JVM-wide settings. Apart from
        // the depth and the entity sizes, these are the defaults of JDK 11 to 21.
        // Each element can add an object and an array to the variant, so 500 levels give at most
        // the 1000 levels that PARSE_JSON allows.
        setLimit(factory, "maxElementDepth", 500);
        setLimit(factory, "entityExpansionLimit", 64_000);
        setLimit(factory, "totalEntitySizeLimit", SIZE_LIMIT);
        setLimit(factory, "maxGeneralEntitySizeLimit", SIZE_LIMIT);
        setLimit(factory, "maxParameterEntitySizeLimit", 1_000_000);
        setLimit(factory, "elementAttributeLimit", 10_000);
        setLimit(factory, "maxXMLNameLimit", 1_000);
        return factory;
    }

    private static void setLimit(XMLInputFactory factory, String limit, int value) {
        factory.setProperty("http://www.oracle.com/xml/jaxp/properties/" + limit, value);
    }

    private XmlElement readDocument(String xml) throws XMLStreamException {
        final XMLStreamReader reader = inputFactory.createXMLStreamReader(new StringReader(xml));
        try {
            final String version = reader.getVersion();
            if (!XmlVersion.isSupported(version)) {
                throw new XMLStreamException(
                        String.format("XML %s documents are not supported.", version));
            }
            while (reader.next() != XMLStreamConstants.START_ELEMENT) {
                // Skip the prolog.
            }
            final XmlElement root = readElement(reader);
            // Lets the reader reject content after the root element.
            while (reader.hasNext()) {
                reader.next();
            }
            return root;
        } finally {
            reader.close();
        }
    }

    private static XmlElement readElement(XMLStreamReader reader) throws XMLStreamException {
        final XmlElement element =
                new XmlElement(qualifiedName(reader.getPrefix(), reader.getLocalName()));
        readAttributes(reader, element);
        final StringBuilder textRun = new StringBuilder();
        for (int event = reader.next();
                event != XMLStreamConstants.END_ELEMENT;
                event = reader.next()) {
            switch (event) {
                case XMLStreamConstants.START_ELEMENT:
                    // A child element ends the current text run.
                    element.addTextRun(textRun);
                    element.addChild(readElement(reader));
                    break;
                case XMLStreamConstants.CHARACTERS:
                case XMLStreamConstants.CDATA:
                case XMLStreamConstants.SPACE:
                    textRun.append(reader.getText());
                    break;
                default:
                    // Comments and processing instructions don't end the text run.
                    break;
            }
        }
        element.addTextRun(textRun);
        element.end();
        return element;
    }

    private static void readAttributes(XMLStreamReader reader, XmlElement element) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            element.addAttribute(
                    qualifiedName(reader.getAttributePrefix(i), reader.getAttributeLocalName(i)),
                    reader.getAttributeValue(i));
        }
    }

    // The reader splits attribute names into prefix and local name, even if not namespace-aware.
    private static String qualifiedName(@Nullable String prefix, String localName) {
        return prefix == null || prefix.isEmpty() ? localName : prefix + ':' + localName;
    }

    /**
     * The XML versions that can be parsed. XML 1.1 isn't supported, since it allows characters that
     * XML 1.0 can't represent, e.g. most control characters. Accepting only XML 1.0 keeps every
     * result representable as XML 1.0.
     */
    private enum XmlVersion {
        XML_1_0("1.0");

        private final String version;

        XmlVersion(String version) {
            this.version = version;
        }

        /** A document without an XML declaration is XML 1.0. */
        static boolean isSupported(@Nullable String version) {
            return version == null
                    || Arrays.stream(values())
                            .anyMatch(supported -> supported.version.equals(version));
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Parses the text of an element with {@code xsi:type}. */
    private static final class XmlSchemaTypes {

        // Parsing a BigInteger or BigDecimal is quadratic in the number of digits.
        private static final int MAX_NUMBER_LENGTH = 1000;

        private XmlSchemaTypes() {}

        /**
         * Returns how to write the text as a value of the type, or null if the type is unknown or
         * the text isn't a valid value of it. The value is written later by the {@link
         * VariantEncoder}, but whether the type applies has to be known while reading.
         */
        static @Nullable Consumer<BinaryVariantInternalBuilder> parse(String text, String xsiType) {
            try {
                switch (localPart(xsiType.trim())) {
                    case "string":
                        return builder -> builder.appendString(text);
                    case "boolean":
                        {
                            final Boolean value = parseBoolean(text);
                            return value == null ? null : builder -> builder.appendBoolean(value);
                        }
                    case "byte":
                        {
                            final byte value = Byte.parseByte(text);
                            return builder -> builder.appendByte(value);
                        }
                    case "short":
                        {
                            final short value = Short.parseShort(text);
                            return builder -> builder.appendShort(value);
                        }
                    case "int":
                        {
                            final int value = Integer.parseInt(text);
                            return builder -> builder.appendInt(value);
                        }
                    case "long":
                        {
                            final long value = Long.parseLong(text);
                            return builder -> builder.appendLong(value);
                        }
                    case "integer":
                        {
                            // BigInteger rejects a fractional part. The variant has no unbounded
                            // integer type, so an integer is stored as a decimal.
                            final BigDecimal value =
                                    toVariantDecimal(
                                            new BigDecimal(
                                                    new BigInteger(checkNumberLength(text))));
                            return builder -> builder.appendDecimal(value);
                        }
                    case "decimal":
                        {
                            final BigDecimal value =
                                    toVariantDecimal(new BigDecimal(checkNumberLength(text)));
                            return builder -> builder.appendDecimal(value);
                        }
                    case "float":
                        {
                            // Like in PARSE_JSON, NaN and infinity aren't typed. A number out of
                            // range parses as infinity.
                            final float value = Float.parseFloat(text);
                            return Float.isFinite(value)
                                    ? builder -> builder.appendFloat(value)
                                    : null;
                        }
                    case "double":
                        {
                            final double value = Double.parseDouble(text);
                            return Double.isFinite(value)
                                    ? builder -> builder.appendDouble(value)
                                    : null;
                        }
                    case "date":
                        {
                            final int days = Math.toIntExact(LocalDate.parse(text).toEpochDay());
                            return builder -> builder.appendDate(days);
                        }
                    case "time":
                        {
                            // A variant time has microsecond precision, so nanoseconds are dropped
                            // like in a cast.
                            final long micros = LocalTime.parse(text).toNanoOfDay() / 1_000;
                            return builder -> builder.appendTime(micros);
                        }
                    case "dateTime":
                        return dateTime(DateTimeFormatter.ISO_DATE_TIME.parse(text));
                    default:
                        return null;
                }
            } catch (NumberFormatException
                    | DateTimeException
                    | ArithmeticException
                    | VariantTypeException e) {
                return null;
            }
        }

        private static String localPart(String type) {
            return type.substring(type.lastIndexOf(':') + 1);
        }

        private static String checkNumberLength(String text) {
            if (text.length() > MAX_NUMBER_LENGTH) {
                throw new NumberFormatException("The number is too long.");
            }
            return text;
        }

        static @Nullable Boolean parseBoolean(String text) {
            switch (text) {
                case "true":
                case "1":
                    return true;
                case "false":
                case "0":
                    return false;
                default:
                    return null;
            }
        }

        private static Consumer<BinaryVariantInternalBuilder> dateTime(TemporalAccessor dateTime) {
            final boolean hasOffset = dateTime.isSupported(ChronoField.OFFSET_SECONDS);
            final Instant instant =
                    hasOffset
                            ? OffsetDateTime.from(dateTime).toInstant()
                            : LocalDateTime.from(dateTime).toInstant(ZoneOffset.UTC);
            // Nanoseconds are kept if the timestamp is within the range of nanosecond timestamps,
            // from 1677 to 2262. Otherwise, they are dropped like in a cast.
            if (instant.getNano() % 1_000 != 0) {
                try {
                    final long nanos = nanosSinceEpoch(instant);
                    return hasOffset
                            ? builder -> builder.appendTimestampLtzNanos(nanos)
                            : builder -> builder.appendTimestampNanos(nanos);
                } catch (VariantTypeException e) {
                    // Out of range, so the timestamp is written with microseconds below.
                }
            }
            final long micros = microsSinceEpoch(instant);
            return hasOffset
                    ? builder -> builder.appendTimestampLtz(micros)
                    : builder -> builder.appendTimestamp(micros);
        }
    }

    // --------------------------------------------------------------------------------------------

    /** An element as read from the document. */
    static final class XmlElement {

        final String name;

        final Map<String, String> attributes = new LinkedHashMap<>();

        // The child elements, grouped by name, and the text runs. Each knows its position among
        // the content, for the # field.
        final Map<String, List<XmlElement>> children = new LinkedHashMap<>();
        final List<TextRun> textRuns = new ArrayList<>();
        private int contentCount;

        // The position among the content of the parent element.
        int position;

        // Set by end() from xsi:nil and xsi:type.
        boolean isNull;
        @Nullable Consumer<BinaryVariantInternalBuilder> typedText;

        private boolean xsiNil;
        private @Nullable String xsiType;

        XmlElement(String name) {
            this.name = name;
        }

        void addAttribute(String name, String value) {
            switch (name) {
                case XSI_NAMESPACE_DECLARATION:
                    // The xsi prefix is recognized without the declaration.
                    break;
                case XSI_NIL:
                    readXsiNil(value);
                    break;
                case XSI_TYPE:
                    xsiType = value;
                    break;
                default:
                    attributes.put(name, value);
            }
        }

        /**
         * xsi:nil="true" marks the element as nil, and xsi:nil="false" has no effect. Any other
         * value isn't a boolean, so it is kept as a regular attribute.
         */
        private void readXsiNil(String value) {
            final Boolean nil = XmlSchemaTypes.parseBoolean(value.trim());
            if (nil == null) {
                attributes.put(XSI_NIL, value);
            } else {
                xsiNil = nil;
            }
        }

        /** Adds the text run, unless it is whitespace only, and clears it for the next one. */
        void addTextRun(StringBuilder textRun) {
            final String trimmed = textRun.toString().trim();
            if (!trimmed.isEmpty()) {
                textRuns.add(new TextRun(trimmed, contentCount++));
            }
            textRun.setLength(0);
        }

        void addChild(XmlElement child) {
            child.position = contentCount++;
            children.computeIfAbsent(child.name, name -> new ArrayList<>()).add(child);
        }

        /**
         * Applies xsi:nil and xsi:type once the element is read, since they depend on all of it.
         */
        void end() {
            if (xsiNil) {
                applyXsiNil();
            } else if (xsiType != null) {
                applyXsiType();
            }
        }

        /**
         * xsi:nil="true" drops the content. The element is null, unless it has other attributes.
         * Then it keeps xsi:nil and xsi:type as attributes.
         */
        private void applyXsiNil() {
            children.clear();
            textRuns.clear();
            if (attributes.isEmpty()) {
                isNull = true;
            } else {
                attributes.put(XSI_NIL, "true");
                if (xsiType != null) {
                    attributes.put(XSI_TYPE, xsiType);
                }
            }
        }

        /**
         * xsi:type types the text of an element with text but without child elements. Otherwise, or
         * if the text isn't a valid value of the type, xsi:type is kept as an attribute.
         */
        private void applyXsiType() {
            if (children.isEmpty() && !textRuns.isEmpty()) {
                typedText = XmlSchemaTypes.parse(text(), xsiType);
            }
            if (typedText == null) {
                attributes.put(XSI_TYPE, xsiType);
            }
        }

        /** Returns the text of an element without child elements. */
        String text() {
            return textRuns.isEmpty() ? "" : textRuns.get(0).text;
        }
    }

    /** A text run of an element, which child elements separate from the next one. */
    private static final class TextRun {

        final String text;

        // The position among the content of the element.
        final int position;

        TextRun(String text, int position) {
            this.text = text;
            this.position = position;
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Writes a tree of {@link XmlElement}s into a variant.
     *
     * <p>The builder writes the values of an object or array first, and its header last, in {@code
     * finishWritingObject} or {@code finishWritingArray}. {@link #addField} records where each
     * field of an object starts, for the header.
     */
    private static final class VariantEncoder {

        private final BinaryVariantInternalBuilder builder =
                new BinaryVariantInternalBuilder(false);
        private final boolean forceArray;

        VariantEncoder(boolean forceArray) {
            this.forceArray = forceArray;
        }

        /** The root element is a single field, never an array, since a document has only one. */
        Variant encodeDocument(XmlElement root) {
            final int start = builder.getWritePos();
            final ArrayList<FieldEntry> fields = new ArrayList<>(1);
            addField(fields, start, root.name);
            encodeElement(root);
            builder.finishWritingObject(start, fields);
            return builder.build();
        }

        /** An element without attributes and child elements is its text, any other an object. */
        private void encodeElement(XmlElement element) {
            if (element.isNull) {
                builder.appendNull();
            } else if (element.attributes.isEmpty() && element.children.isEmpty()) {
                encodeText(element, element.text());
            } else {
                encodeObject(element);
            }
        }

        private void encodeObject(XmlElement element) {
            final int start = builder.getWritePos();
            final ArrayList<FieldEntry> fields = new ArrayList<>();
            for (Map.Entry<String, String> attribute : element.attributes.entrySet()) {
                addField(fields, start, ATTRIBUTE_PREFIX + attribute.getKey());
                builder.appendString(attribute.getValue());
            }
            for (Map.Entry<String, List<XmlElement>> children : element.children.entrySet()) {
                addField(fields, start, children.getKey());
                encodeItems(children.getValue(), this::encodeElement);
            }
            if (!element.textRuns.isEmpty()) {
                addField(fields, start, TEXT_KEY);
                encodeItems(element.textRuns, textRun -> encodeText(element, textRun.text));
            }
            // The # field records the order of the content across keys. Within a key, it is the
            // order of the array, so with a single key, there is nothing to record.
            final int contentKeys = element.children.size() + (element.textRuns.isEmpty() ? 0 : 1);
            if (contentKeys > 1) {
                addField(fields, start, ORDER_KEY);
                encodeOrder(element);
            }
            builder.finishWritingObject(start, fields);
        }

        private void encodeOrder(XmlElement element) {
            final int start = builder.getWritePos();
            final ArrayList<FieldEntry> fields = new ArrayList<>();
            for (Map.Entry<String, List<XmlElement>> children : element.children.entrySet()) {
                addField(fields, start, children.getKey());
                encodeItems(children.getValue(), child -> builder.appendNumeric(child.position));
            }
            if (!element.textRuns.isEmpty()) {
                addField(fields, start, TEXT_KEY);
                encodeItems(element.textRuns, textRun -> builder.appendNumeric(textRun.position));
            }
            builder.finishWritingObject(start, fields);
        }

        /**
         * Writes the items of a key: a single item as is, and several items as an array. With
         * {@code forceArray}, a single item is an array as well.
         */
        private <T> void encodeItems(List<T> items, Consumer<T> encodeItem) {
            if (items.size() == 1 && !forceArray) {
                encodeItem.accept(items.get(0));
                return;
            }
            final int start = builder.getWritePos();
            final ArrayList<Integer> offsets = new ArrayList<>(items.size());
            for (T item : items) {
                offsets.add(builder.getWritePos() - start);
                encodeItem.accept(item);
            }
            builder.finishWritingArray(start, offsets);
        }

        private void addField(ArrayList<FieldEntry> fields, int start, String key) {
            fields.add(new FieldEntry(key, builder.addKey(key), builder.getWritePos() - start));
        }

        // Only an element without child elements has typed text. It has at most one text run, which
        // the typed value replaces.
        private void encodeText(XmlElement element, String text) {
            if (element.typedText != null) {
                element.typedText.accept(builder);
            } else {
                builder.appendString(text);
            }
        }
    }
}
