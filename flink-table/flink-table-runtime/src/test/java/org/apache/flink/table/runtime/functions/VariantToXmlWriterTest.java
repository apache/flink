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

import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Tests for {@link VariantToXmlWriter}. */
class VariantToXmlWriterTest {

    private static final String XSI = "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"";
    private static final String XSI_AND_XS = XSI + " xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"";

    private static final VariantBuilder BUILDER = Variant.newBuilder();

    private final XmlToVariantParser parser = new XmlToVariantParser();

    // --------------------------------------------------------------------------------------------
    // Variants from PARSE_XML
    // --------------------------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("parsedDocuments")
    void testWriteParsedDocument(String xml, String expectedXml) {
        assertThat(VariantToXmlWriter.write(parser.parse(xml, false))).isEqualTo(expectedXml);
    }

    static Stream<Arguments> parsedDocuments() {
        return Stream.of(
                // The result is the normalized document, with the content in the order of the #
                // field and the attributes in the order of their keys.
                arguments(
                        "<?xml version=\"1.0\"?><!-- c --><book pages=\"320\" id=\"1\">\n"
                                + "  <title>Dune</title>\n"
                                + "  This is a description\n"
                                + "  <!-- c --><price>12.50</price><?pi x?>\n"
                                + "</book>",
                        "<book id=\"1\" pages=\"320\"><title>Dune</title>This is a description"
                                + "<price>12.50</price></book>"),
                // An empty string is an empty element.
                arguments("<title/>", "<title></title>"),
                // Namespace prefixes and declarations are kept.
                arguments(
                        "<ns:a xmlns:ns=\"urn:ns\" ns:id=\"1\"><ns:b>x</ns:b></ns:a>",
                        "<ns:a ns:id=\"1\" xmlns:ns=\"urn:ns\"><ns:b>x</ns:b></ns:a>"),
                // A NULL, and the null text of an element with attributes, are nil.
                arguments(
                        "<r><a xsi:nil=\"true\"/><b xsi:nil=\"true\" id=\"1\">x</b></r>",
                        "<r " + XSI + "><a xsi:nil=\"true\"/><b id=\"1\" xsi:nil=\"true\"/></r>"),
                arguments("<a xsi:nil=\"true\"/>", "<a " + XSI + " xsi:nil=\"true\"/>"),
                // Typed values are written in the lexical form of their type.
                arguments(
                        "<r>"
                                + "<a xsi:type=\"boolean\">1</a>"
                                + "<b xsi:type=\"xs:int\">+5</b>"
                                + "<c xsi:type=\"integer\">007</c>"
                                + "<d xsi:type=\"double\">1.5E3</d>"
                                + "<e xsi:type=\"time\">12:30:00.500</e>"
                                + "<f xsi:type=\"dateTime\">2026-09-23T12:30:45.5+02:00</f>"
                                + "<g xsi:type=\"dateTime\">2026-09-23T12:30:45</g>"
                                + "<h xsi:type=\"base64Binary\">AQ I D</h>"
                                + "</r>",
                        "<r "
                                + XSI_AND_XS
                                + "><a xsi:type=\"xs:boolean\">true</a>"
                                + "<b xsi:type=\"xs:int\">5</b>"
                                + "<c xsi:type=\"xs:decimal\">7</c>"
                                + "<d xsi:type=\"xs:double\">1500.0</d>"
                                + "<e xsi:type=\"xs:time\">12:30:00.5</e>"
                                + "<f xsi:type=\"xs:dateTime\">2026-09-23T10:30:45.5Z</f>"
                                + "<g xsi:type=\"xs:dateTime\">2026-09-23T12:30:45</g>"
                                + "<h xsi:type=\"xs:base64Binary\">AQID</h>"
                                + "</r>"),
                arguments(
                        "<price xsi:type=\"decimal\" currency=\"EUR\">13.50</price>",
                        "<price "
                                + XSI_AND_XS
                                + " currency=\"EUR\" xsi:type=\"xs:decimal\">13.5</price>"),
                // An xsi:type that is kept as an attribute is written as it is.
                arguments(
                        "<a xsi:type=\"int\">five</a>", "<a " + XSI + " xsi:type=\"int\">five</a>"),
                // The declarations with the namespaces of XML Schema are dropped and declared
                // again where needed, and other declarations are kept.
                arguments(
                        "<r xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><a>x</a></r>",
                        "<r><a>x</a></r>"),
                arguments(
                        "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
                                + "<xs:element name=\"Book\"/></xs:schema>",
                        "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
                                + "<xs:element name=\"Book\"></xs:element></xs:schema>"),
                arguments(
                        "<r xmlns:xs=\"urn:custom\"><a xsi:type=\"int\">5</a></r>",
                        "<r " + XSI + " xmlns:xs=\"urn:custom\"><a xsi:type=\"xs:int\">5</a></r>"),
                // A declaration on another element doesn't apply to the root element.
                arguments(
                        "<r><a xmlns:xsi=\"urn:custom\" xsi:nil=\"true\"/></r>",
                        "<r " + XSI + "><a xmlns:xsi=\"urn:custom\" xsi:nil=\"true\"/></r>"));
    }

    /** {@code PARSE_XML(XML_STRING(v))} is {@code v} for variants from {@code PARSE_XML}. */
    @ParameterizedTest(name = "{0}")
    @ValueSource(
            strings = {
                "<book pages=\"320\"><title>Dune</title>This is a description"
                        + "<price>12.50</price>Text in between<price currency=\"EUR\">13.50"
                        + "</price></book>",
                "<a><b>1</b><c>2</c><b>3</b><c>4</c>text<b>5</b></a>",
                "<ns:a xmlns:ns=\"urn:ns\" xmlns=\"urn:default\" ns:id=\"1\"><ns:b>x</ns:b></ns:a>",
                "<a b=\"&quot;&lt;&amp;&gt; x \">&lt;&amp;&gt;\"&#9;x&#10;y</a>",
                "<book><a xsi:nil=\"true\" id=\"1\"/><a>x</a><a xsi:nil=\"true\"/></book>",
                "<a xsi:type=\"int\">five</a>",
                "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
                        + "<xs:element name=\"Book\" type=\"xs:string\"/></xs:schema>",
                "<r xmlns:xs=\"urn:custom\"><a xsi:type=\"int\">5</a></r>",
                "<r><a xmlns:xsi=\"urn:custom\" xsi:nil=\"true\"/></r>",
                "<r>"
                        + "<a xsi:type=\"boolean\">true</a>"
                        + "<b xsi:type=\"byte\">-1</b>"
                        + "<c xsi:type=\"short\">300</c>"
                        + "<d xsi:type=\"int\">70000</d>"
                        + "<e xsi:type=\"long\">5000000000</e>"
                        + "<f xsi:type=\"integer\">123456789012345678901234567890</f>"
                        + "<g xsi:type=\"decimal\">-12.50</g>"
                        + "<h xsi:type=\"float\">1.1</h>"
                        + "<i xsi:type=\"double\">1.0E-300</i>"
                        + "<j xsi:type=\"date\">2026-09-23</j>"
                        + "<k xsi:type=\"time\">12:30:45.123456</k>"
                        + "<l xsi:type=\"dateTime\">2026-09-23T12:30:45.123456</l>"
                        + "<m xsi:type=\"dateTime\">2026-09-23T12:30:45.123456789</m>"
                        + "<n xsi:type=\"dateTime\">2026-09-23T12:30:45.123456+02:00</n>"
                        + "<o xsi:type=\"dateTime\">2026-09-23T12:30:45.123456789Z</o>"
                        + "<p xsi:type=\"string\">5</p>"
                        + "<q xsi:type=\"base64Binary\">AQID</q>"
                        + "</r>",
                // The limits of the types.
                "<r>"
                        + "<a xsi:type=\"date\">9999-12-31</a>"
                        + "<b xsi:type=\"date\">-5877641-06-23</b>"
                        + "<c xsi:type=\"dateTime\">9999-12-31T23:59:59.999999Z</c>"
                        + "<d xsi:type=\"dateTime\">2262-04-11T23:47:16.854775807</d>"
                        + "<e xsi:type=\"time\">23:59:59.999999</e>"
                        + "<f xsi:type=\"double\">4.9E-324</f>"
                        + "<g xsi:type=\"float\">3.4028235E38</g>"
                        + "</r>"
            })
    void testRoundTrip(String xml) {
        for (boolean forceArray : new boolean[] {false, true}) {
            final Variant variant = parser.parse(xml, forceArray);
            final String written = VariantToXmlWriter.write(variant);
            final Variant reparsed = parser.parse(written, forceArray);
            assertThat(describe(reparsed)).as(written).isEqualTo(describe(variant));
            // The written document is normalized, so writing it again gives the same document.
            assertThat(VariantToXmlWriter.write(reparsed)).isEqualTo(written);
        }
    }

    /**
     * Tabs and line breaks in attribute values, and carriage returns in text, are written as they
     * are, so a parser reads them back as spaces and line feeds.
     */
    @Test
    void testWhitespaceThatParsersNormalize() {
        final Variant variant = parser.parse("<a b=\"x&#9;y&#10;z&#13;w\">x&#13;y</a>", false);
        assertThat(parser.parse(VariantToXmlWriter.write(variant), false).toJson())
                .isEqualTo("{\"a\":{\"$\":\"x\\ny\",\"@b\":\"x y z w\"}}");
    }

    // --------------------------------------------------------------------------------------------
    // Scalars
    // --------------------------------------------------------------------------------------------

    @ParameterizedTest(name = "{1}")
    @MethodSource({"typedScalars", "untypedScalars"})
    void testWriteScalar(Variant scalar, String expectedXml) {
        assertThat(VariantToXmlWriter.write(BUILDER.object().add("a", scalar).build()))
                .isEqualTo(expectedXml);
    }

    static Stream<Arguments> typedScalars() {
        return Stream.of(
                arguments(BUILDER.of(true), typed("boolean", "true")),
                arguments(BUILDER.of((byte) -1), typed("byte", "-1")),
                arguments(BUILDER.of((short) 300), typed("short", "300")),
                arguments(BUILDER.of(70000), typed("int", "70000")),
                arguments(BUILDER.of(5000000000L), typed("long", "5000000000")),
                arguments(BUILDER.of(new BigDecimal("1E+3")), typed("decimal", "1000")),
                arguments(BUILDER.of(1.1f), typed("float", "1.1")),
                arguments(BUILDER.of(1.0E-300), typed("double", "1.0E-300")),
                // XML Schema writes infinity as INF.
                arguments(BUILDER.of(Float.NaN), typed("float", "NaN")),
                arguments(BUILDER.of(Double.NEGATIVE_INFINITY), typed("double", "-INF")),
                // Years after 9999 have no + sign.
                arguments(BUILDER.of(LocalDate.of(10000, 1, 1)), typed("date", "10000-01-01")),
                arguments(BUILDER.of(LocalDate.of(-1, 1, 1)), typed("date", "-0001-01-01")),
                // The seconds are always written.
                arguments(BUILDER.of(LocalTime.of(12, 30)), typed("time", "12:30:00")),
                arguments(
                        BUILDER.of(LocalDateTime.of(2026, 9, 23, 12, 30, 45, 123_456_789)),
                        typed("dateTime", "2026-09-23T12:30:45.123456789")),
                // A TIMESTAMP_LTZ is written in UTC.
                arguments(
                        BUILDER.of(Instant.parse("2026-09-23T10:30:00Z")),
                        typed("dateTime", "2026-09-23T10:30:00Z")),
                arguments(BUILDER.of(new byte[] {1, 2, 3}), typed("base64Binary", "AQID")));
    }

    static Stream<Arguments> untypedScalars() {
        return Stream.of(
                arguments(BUILDER.of("x"), "<a>x</a>"),
                // XML Schema has no type for a UUID.
                arguments(
                        BUILDER.of(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")),
                        "<a>123e4567-e89b-12d3-a456-426614174000</a>"));
    }

    private static String typed(String xsiType, String text) {
        return String.format("<a %s xsi:type=\"xs:%s\">%s</a>", XSI_AND_XS, xsiType, text);
    }

    // --------------------------------------------------------------------------------------------
    // Variants without an XML representation
    // --------------------------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("variantsWithoutXmlRepresentation")
    void testNoXmlRepresentation(String json, String expectedMessage) throws IOException {
        final Variant variant = parseJson(json);
        assertThatThrownBy(() -> VariantToXmlWriter.write(variant))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage);
    }

    static Stream<Arguments> variantsWithoutXmlRepresentation() {
        return Stream.of(
                // The root element
                arguments(
                        "1",
                        "The variant must be an object with a single field for the root element, "
                                + "but it is of type TINYINT."),
                arguments(
                        "{\"a\":1,\"b\":2}",
                        "The variant must be an object with a single field for the root element, "
                                + "but it has 2 fields."),
                arguments(
                        "{\"a\":[\"x\",\"y\"]}",
                        "The root element 'a' can't be an array, since a document has only one "
                                + "root element."),
                arguments(
                        "{\"r\":{\"a\":[[\"x\"]]}}",
                        "The field 'a' of the element 'r' has an array nested in an array, which "
                                + "XML can't represent."),
                // Names, so that a key can't add markup
                arguments(
                        "{\"r\":{\"a><evil/><b\":\"x\"}}",
                        "'a><evil/><b' is not a valid XML element name."),
                arguments(
                        "{\"a\":{\"@x=\\\"\\\" evil=\\\"1\\\" y\":\"v\"}}",
                        "'x=\"\" evil=\"1\" y' is not a valid XML attribute name."),
                // Only the names that PARSE_XML reads.
                arguments("{\"\\u2070\":\"x\"}", "'\u2070' is not a valid XML element name."),
                // Attributes and text
                arguments(
                        "{\"a\":{\"@b\":{\"c\":\"x\"}}}",
                        "The attribute 'b' of the element 'a' can't be of type OBJECT."),
                arguments(
                        "{\"a\":{\"$\":{\"c\":\"x\"}}}",
                        "The text of the element 'a' can't be of type OBJECT."),
                // Characters that XML 1.0 can't represent
                arguments(
                        "{\"a\":\"x\\u0000y\"}",
                        "The text of the element 'a' has the character U+0000, which XML 1.0 "
                                + "doesn't allow."),
                arguments(
                        "{\"a\":{\"@b\":\"x\\uFFFEy\"}}",
                        "The attribute 'b' of the element 'a' has the character U+FFFE, which XML "
                                + "1.0 doesn't allow."),
                arguments(
                        "{\"a\":{\"$\":null,\"b\":\"x\"}}",
                        "The element 'a' has a null text and other content, but an element with "
                                + "xsi:nil can't have content."),
                arguments(
                        "{\"a\":{\"@xsi:nil\":\" 1 \",\"b\":\"x\"}}",
                        "The element 'a' has an '@xsi:nil' field that is true and content, but an "
                                + "element with xsi:nil can't have content."),
                // An element can't have an attribute twice.
                arguments(
                        "{\"a\":{\"$\":5,\"@xsi:type\":\"T\"}}",
                        "The element 'a' has an '@xsi:type' field and a typed text, but an element "
                                + "can have only one xsi:type attribute."),
                // The # field
                arguments(
                        "{\"a\":{\"#\":5,\"b\":\"x\"}}",
                        "The '#' field of the element 'a' must be an object, but it is of type "
                                + "TINYINT."),
                arguments(
                        "{\"a\":{\"#\":{\"b\":[0,1]},\"b\":\"x\"}}",
                        "The number of positions for 'b' in the '#' field of the element 'a' is 2, "
                                + "but the number of items is 1."),
                arguments(
                        "{\"a\":{\"#\":{\"b\":0,\"c\":0},\"b\":\"x\",\"c\":\"y\"}}",
                        "The '#' field of the element 'a' has the position 0 more than once."),
                arguments(
                        "{\"a\":{\"#\":{\"x\":\"0\"},\"b\":\"y\"}}",
                        "The '#' field of the element 'a' has a position of type STRING for 'x', "
                                + "but a position must be an integer."));
    }

    // --------------------------------------------------------------------------------------------

    private static Variant parseJson(String json) throws IOException {
        return BinaryVariantInternalBuilder.parseJson(json, false);
    }

    /**
     * Describes a variant with the type of each scalar, which {@link Variant#toJson()} doesn't
     * show. Unlike the bytes of a variant, it doesn't depend on the order in which the keys were
     * added, which is the order of the attributes in the document.
     */
    private static String describe(Variant variant) {
        switch (variant.getType()) {
            case OBJECT:
                return variant.getFieldNames().stream()
                        .map(key -> key + "=" + describe(variant.getField(key)))
                        .collect(Collectors.joining(", ", "{", "}"));
            case ARRAY:
                return IntStream.range(0, variant.getArraySize())
                        .mapToObj(i -> describe(variant.getElement(i)))
                        .collect(Collectors.joining(", ", "[", "]"));
            case NULL:
                return "NULL";
            case BYTES:
                return "BYTES " + Arrays.toString(variant.getBytes());
            default:
                return variant.getType() + " " + variant.get();
        }
    }
}
