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

import org.apache.flink.types.variant.Variant;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.xml.sax.SAXException;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;

import java.io.StringReader;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Tests for {@link XmlToVariantParser}. */
class XmlToVariantParserTest {

    private final XmlToVariantParser parser = new XmlToVariantParser();

    /** How elements, attributes, and text map to a variant. */
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class Mapping {

        @ParameterizedTest(name = "{0}")
        @MethodSource("mappings")
        void testMapping(String xml, String expectedJson) {
            assertThat(parser.parse(xml, false).toJson()).isEqualTo(expectedJson);
        }

        Stream<Arguments> mappings() {
            return Stream.of(
                    arguments("<title>Dune</title>", "{\"title\":\"Dune\"}"),
                    arguments("<title></title>", "{\"title\":\"\"}"),
                    arguments("<title/>", "{\"title\":\"\"}"),
                    arguments(
                            "<book pages=\"320\">\n Dune\n</book>",
                            "{\"book\":{\"$\":\"Dune\",\"@pages\":\"320\"}}"),
                    arguments("<book pages=\"320\"/>", "{\"book\":{\"@pages\":\"320\"}}"),
                    arguments(
                            "<book>\n"
                                    + "  <author>Terry Pratchett</author>\n"
                                    + "  <author>Neil Gaiman</author>\n"
                                    + "</book>",
                            "{\"book\":{\"author\":[\"Terry Pratchett\",\"Neil Gaiman\"]}}"),
                    arguments(
                            "<book>\n  <title>Dune</title>\n  <price>12.5</price>\n</book>",
                            "{\"book\":{\"#\":{\"price\":1,\"title\":0},\"price\":\"12.5\",\"title\":\"Dune\"}}"),
                    arguments(
                            "<book>\n"
                                    + "  This is a description\n"
                                    + "  <title>Dune</title>\n"
                                    + "  and this is another text\n"
                                    + "</book>",
                            "{\"book\":{\"#\":{\"$\":[0,2],\"title\":1},"
                                    + "\"$\":[\"This is a description\",\"and this is another text\"],"
                                    + "\"title\":\"Dune\"}}"),
                    arguments(
                            "<book pages=\"320\">\n"
                                    + "  <title>Dune</title>\n"
                                    + "  This is a description\n"
                                    + "  <price>12.50</price>\n"
                                    + "  Text in between\n"
                                    + "  <price currency=\"EUR\">13.50</price>\n"
                                    + "</book>",
                            "{\"book\":{\"#\":{\"$\":[1,3],\"price\":[2,4],\"title\":0},"
                                    + "\"$\":[\"This is a description\",\"Text in between\"],"
                                    + "\"@pages\":\"320\","
                                    + "\"price\":[\"12.50\",{\"$\":\"13.50\",\"@currency\":\"EUR\"}],"
                                    + "\"title\":\"Dune\"}}"),
                    arguments("<a><b><c>deep</c></b></a>", "{\"a\":{\"b\":{\"c\":\"deep\"}}}"),
                    // Namespace prefixes and declarations are kept as written.
                    arguments(
                            "<ns:a xmlns:ns=\"urn:ns\" xmlns=\"urn:default\" ns:id=\"1\">"
                                    + "<ns:b>x</ns:b></ns:a>",
                            "{\"ns:a\":{\"@ns:id\":\"1\",\"@xmlns\":\"urn:default\","
                                    + "\"@xmlns:ns\":\"urn:ns\",\"ns:b\":\"x\"}}"),
                    // Attribute values are not trimmed.
                    arguments("<a b=\" x \"/>", "{\"a\":{\"@b\":\" x \"}}"),
                    // CDATA is text, and it merges with the text around it.
                    arguments("<a><![CDATA[<b>&</b>]]></a>", "{\"a\":\"<b>&</b>\"}"),
                    arguments("<a>x<![CDATA[y]]>z</a>", "{\"a\":\"xyz\"}"),
                    // Comments and processing instructions are dropped, the text around them
                    // merges.
                    arguments(
                            "<?xml version=\"1.0\"?><!-- c --><a>foo<!-- c -->bar<?pi x?></a><!-- c -->",
                            "{\"a\":\"foobar\"}"),
                    // The input is a string, so an encoding declared in the document is ignored.
                    arguments(
                            "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><a>\u00e4</a>",
                            "{\"a\":\"\u00e4\"}"),
                    // Whitespace-only text is dropped, other text is trimmed of XML whitespace
                    // only.
                    arguments("<a>\n  <b>x</b>\n</a>", "{\"a\":{\"b\":\"x\"}}"),
                    arguments("<a> \t\r\n </a>", "{\"a\":\"\"}"),
                    arguments("<a>\u00a0x\u00a0</a>", "{\"a\":\"\u00a0x\u00a0\"}"),
                    // Predefined entities and character references are expanded, in text and in
                    // attribute values.
                    arguments("<a>&lt;&gt;&amp;&apos;</a>", "{\"a\":\"<>&'\"}"),
                    arguments("<a>&#65;&#x42;</a>", "{\"a\":\"AB\"}"),
                    arguments("<a b=\"&lt;&#65;\"/>", "{\"a\":{\"@b\":\"<A\"}}"),
                    // Entities declared in the DTD are expanded.
                    arguments(
                            "<!DOCTYPE a [<!ENTITY title \"Dune\">]><a>&title;</a>",
                            "{\"a\":\"Dune\"}"));
        }
    }

    /** How {@code xsi:nil} and {@code xsi:type} change the mapping. */
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class XsiAttributes {

        @ParameterizedTest(name = "{0}")
        @MethodSource("xsiMappings")
        void testXsiMapping(String xml, String expectedJson) {
            assertThat(parser.parse(xml, false).toJson()).isEqualTo(expectedJson);
        }

        Stream<Arguments> xsiMappings() {
            return Stream.of(
                    // xsi:nil makes an element without other attributes null, and drops the
                    // content.
                    arguments("<a xsi:nil=\"true\"/>", "{\"a\":null}"),
                    arguments("<a xsi:nil=\"1\"></a>", "{\"a\":null}"),
                    arguments("<a xsi:nil=\"true\">x</a>", "{\"a\":null}"),
                    arguments("<a xsi:nil=\"true\"><b>x</b></a>", "{\"a\":null}"),
                    arguments("<a xsi:nil=\" true \" xsi:type=\"int\"/>", "{\"a\":null}"),
                    arguments("<a xsi:nil=\"true\" xsi:type=\"Dog\"/>", "{\"a\":null}"),
                    // The declarations of the xsi and xs prefixes with the namespaces of XML
                    // Schema are dropped, and other declarations are kept.
                    arguments(
                            "<root xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
                                    + "<a xsi:nil=\"true\"/></root>",
                            "{\"root\":{\"a\":null}}"),
                    arguments(
                            "<root xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">"
                                    + "<a xsi:type=\"xs:int\">5</a></root>",
                            "{\"root\":{\"a\":5}}"),
                    arguments(
                            "<root xmlns:xsi=\"urn:custom\" xmlns:xs=\"urn:custom\">"
                                    + "<a xsi:type=\"xs:int\">5</a></root>",
                            "{\"root\":{\"@xmlns:xs\":\"urn:custom\","
                                    + "\"@xmlns:xsi\":\"urn:custom\",\"a\":5}}"),
                    arguments(
                            "<root xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"/>",
                            "{\"root\":{\"@xmlns:xsd\":\"http://www.w3.org/2001/XMLSchema\"}}"),
                    // With other attributes, the text of a nil element is null.
                    arguments(
                            "<a xsi:nil=\"true\" id=\"1\"/>", "{\"a\":{\"$\":null,\"@id\":\"1\"}}"),
                    arguments(
                            "<a xsi:nil=\"true\" id=\"1\">x</a>",
                            "{\"a\":{\"$\":null,\"@id\":\"1\"}}"),
                    arguments(
                            "<a xsi:nil=\"true\" id=\"1\"><b>x</b></a>",
                            "{\"a\":{\"$\":null,\"@id\":\"1\"}}"),
                    arguments(
                            "<a xsi:nil=\" 1 \" id=\"1\"/>", "{\"a\":{\"$\":null,\"@id\":\"1\"}}"),
                    arguments(
                            "<a xsi:nil=\"true\" xsi:type=\"string\" id=\"1\"/>",
                            "{\"a\":{\"$\":null,\"@id\":\"1\"}}"),
                    // xsi:nil that isn't a boolean is a regular attribute, and false is dropped.
                    arguments("<a xsi:nil=\"yes\"/>", "{\"a\":{\"@xsi:nil\":\"yes\"}}"),
                    arguments("<a xsi:nil=\"TRUE\"/>", "{\"a\":{\"@xsi:nil\":\"TRUE\"}}"),
                    arguments("<a xsi:nil=\"false\">x</a>", "{\"a\":\"x\"}"),
                    arguments("<a xsi:nil=\"0\"/>", "{\"a\":\"\"}"),
                    // xsi:type types the text of an element with text but without child elements.
                    arguments("<a xsi:type=\"int\">5</a>", "{\"a\":5}"),
                    arguments("<a xsi:type=\"boolean\">true</a>", "{\"a\":true}"),
                    arguments("<a xsi:type=\"decimal\">-1.23</a>", "{\"a\":-1.23}"),
                    arguments(
                            "<price xsi:type=\"decimal\" currency=\"EUR\">13.50</price>",
                            "{\"price\":{\"$\":13.5,\"@currency\":\"EUR\"}}"),
                    // xsi:type that doesn't type the text is kept.
                    arguments("<a xsi:type=\"int\"></a>", "{\"a\":{\"@xsi:type\":\"int\"}}"),
                    arguments("<a xsi:type=\"string\"/>", "{\"a\":{\"@xsi:type\":\"string\"}}"),
                    arguments(
                            "<a id=\"1\" xsi:type=\"string\"/>",
                            "{\"a\":{\"@id\":\"1\",\"@xsi:type\":\"string\"}}"),
                    arguments(
                            "<a xsi:type=\"int\">five</a>",
                            "{\"a\":{\"$\":\"five\",\"@xsi:type\":\"int\"}}"),
                    arguments(
                            "<a xsi:type=\"xs:token\">x</a>",
                            "{\"a\":{\"$\":\"x\",\"@xsi:type\":\"xs:token\"}}"),
                    arguments(
                            "<animal xsi:type=\"Dog\" name=\"Rex\"/>",
                            "{\"animal\":{\"@name\":\"Rex\",\"@xsi:type\":\"Dog\"}}"),
                    arguments(
                            "<shape xsi:type=\"Circle\"><radius>1</radius></shape>",
                            "{\"shape\":{\"@xsi:type\":\"Circle\",\"radius\":\"1\"}}"),
                    // Other xsi: attributes are regular attributes.
                    arguments(
                            "<a xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
                                    + "xsi:schemaLocation=\"urn:a a.xsd\">x</a>",
                            "{\"a\":{\"$\":\"x\",\"@xsi:schemaLocation\":\"urn:a a.xsd\"}}"));
        }
    }

    /** Which texts {@code xsi:type} stores with their type. */
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class XsiTypes {

        @ParameterizedTest(name = "{0}: {1}")
        @MethodSource("typedValues")
        void testXsiType(
                String xsiType, String text, Variant.Type expectedType, Object expectedValue) {
            final Variant value = parseTyped(xsiType, text);
            assertThat(value.getType()).isEqualTo(expectedType);
            if (expectedValue instanceof BigDecimal) {
                assertThat(value.getDecimal()).isEqualByComparingTo((BigDecimal) expectedValue);
            } else {
                assertThat(value.get()).isEqualTo(expectedValue);
            }
        }

        Stream<Arguments> typedValues() {
            return Stream.of(
                    arguments("string", "5", Variant.Type.STRING, "5"),
                    arguments("boolean", "true", Variant.Type.BOOLEAN, true),
                    arguments("boolean", "false", Variant.Type.BOOLEAN, false),
                    arguments("boolean", "1", Variant.Type.BOOLEAN, true),
                    arguments("boolean", "0", Variant.Type.BOOLEAN, false),
                    arguments("byte", "-128", Variant.Type.TINYINT, (byte) -128),
                    arguments("short", "32767", Variant.Type.SMALLINT, (short) 32767),
                    arguments("int", "+5", Variant.Type.INT, 5),
                    arguments("int", " 5 ", Variant.Type.INT, 5),
                    arguments("long", "9223372036854775807", Variant.Type.BIGINT, Long.MAX_VALUE),
                    arguments(
                            "integer",
                            "123456789012345678901234567890",
                            Variant.Type.DECIMAL,
                            new BigDecimal("123456789012345678901234567890")),
                    arguments("decimal", "12.50", Variant.Type.DECIMAL, new BigDecimal("12.50")),
                    arguments("decimal", ".5", Variant.Type.DECIMAL, new BigDecimal("0.5")),
                    // A sign is allowed, also for zero.
                    arguments("decimal", "-0.0", Variant.Type.DECIMAL, new BigDecimal("0.0")),
                    arguments("float", "+0.0", Variant.Type.FLOAT, 0.0f),
                    arguments("double", "-0.0", Variant.Type.DOUBLE, -0.0d),
                    arguments("float", "1.5", Variant.Type.FLOAT, 1.5f),
                    arguments("double", "1.5E3", Variant.Type.DOUBLE, 1500.0),
                    arguments("double", "-.5e+2", Variant.Type.DOUBLE, -50.0),
                    arguments("date", "2026-09-23", Variant.Type.DATE, LocalDate.of(2026, 9, 23)),
                    arguments(
                            "time",
                            "12:30:45.123456",
                            Variant.Type.TIME,
                            LocalTime.of(12, 30, 45, 123_456_000)),
                    // Precision that doesn't fit is dropped, like in a cast.
                    arguments(
                            "time",
                            "12:30:45.123456789",
                            Variant.Type.TIME,
                            LocalTime.of(12, 30, 45, 123_456_000)),
                    arguments(
                            "time",
                            "12:30:45.1234567890123",
                            Variant.Type.TIME,
                            LocalTime.of(12, 30, 45, 123_456_000)),
                    arguments(
                            "dateTime",
                            "2026-09-23T12:30:00.1234567890123456789",
                            Variant.Type.TIMESTAMP_NS,
                            LocalDateTime.of(2026, 9, 23, 12, 30, 0, 123_456_789)),
                    arguments(
                            "dateTime",
                            "2300-01-01T00:00:00.000000001",
                            Variant.Type.TIMESTAMP,
                            LocalDateTime.of(2300, 1, 1, 0, 0)),
                    arguments(
                            "dateTime",
                            "2026-09-23T12:30:45",
                            Variant.Type.TIMESTAMP,
                            LocalDateTime.of(2026, 9, 23, 12, 30, 45)),
                    arguments(
                            "dateTime",
                            "2026-09-23T12:30:45.123456789",
                            Variant.Type.TIMESTAMP_NS,
                            LocalDateTime.of(2026, 9, 23, 12, 30, 45, 123_456_789)),
                    arguments(
                            "dateTime",
                            "2026-09-23T12:30:45Z",
                            Variant.Type.TIMESTAMP_LTZ,
                            Instant.parse("2026-09-23T12:30:45Z")),
                    arguments(
                            "dateTime",
                            "2026-09-23T12:30:45.5+02:00",
                            Variant.Type.TIMESTAMP_LTZ,
                            Instant.parse("2026-09-23T10:30:45.5Z")),
                    arguments(
                            "dateTime",
                            "2026-09-23T12:30:45.123456789Z",
                            Variant.Type.TIMESTAMP_LTZ_NS,
                            Instant.parse("2026-09-23T12:30:45.123456789Z")),
                    arguments("date", "9999-12-31", Variant.Type.DATE, LocalDate.of(9999, 12, 31)),
                    arguments(
                            "dateTime",
                            "9999-12-31T23:59:59.999999",
                            Variant.Type.TIMESTAMP,
                            LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_000)),
                    arguments(
                            "dateTime",
                            "9999-12-31T23:59:59.999999Z",
                            Variant.Type.TIMESTAMP_LTZ,
                            Instant.parse("9999-12-31T23:59:59.999999Z")),
                    arguments(
                            "dateTime",
                            "2262-04-11T23:47:16.854775807",
                            Variant.Type.TIMESTAMP_NS,
                            LocalDateTime.of(2262, 4, 11, 23, 47, 16, 854_775_807)),
                    arguments(
                            "dateTime",
                            "2262-04-11T23:47:16.854775807Z",
                            Variant.Type.TIMESTAMP_LTZ_NS,
                            Instant.parse("2262-04-11T23:47:16.854775807Z")),
                    arguments("base64Binary", "AQID", Variant.Type.BYTES, new byte[] {1, 2, 3}),
                    arguments("base64Binary", "AQ==", Variant.Type.BYTES, new byte[] {1}),
                    arguments("base64Binary", "AQI=", Variant.Type.BYTES, new byte[] {1, 2}),
                    // Whitespace is allowed between the characters, e.g. for line breaks.
                    arguments(
                            "base64Binary",
                            "AQID\nBAUG",
                            Variant.Type.BYTES,
                            new byte[] {1, 2, 3, 4, 5, 6}),
                    arguments("base64Binary", "AQ I D", Variant.Type.BYTES, new byte[] {1, 2, 3}),
                    // Types are matched by their local part.
                    arguments("xs:int", "5", Variant.Type.INT, 5),
                    arguments("xsd:int", "5", Variant.Type.INT, 5));
        }

        @ParameterizedTest(name = "{0}: {1}")
        @MethodSource({"valuesThatStayStrings", "invalidLexicalForms"})
        void testXsiTypeThatDoesNotApply(String xsiType, String text) {
            final Variant value = parseTyped(xsiType, text);
            assertThat(value.getField("$").getString()).isEqualTo(text);
            assertThat(value.getField("@xsi:type").getString()).isEqualTo(xsiType);
        }

        /** Values with a type that doesn't apply, so that they stay strings with @xsi:type. */
        Stream<Arguments> valuesThatStayStrings() {
            return Stream.of(
                    arguments("boolean", "yes"),
                    arguments("boolean", "TRUE"),
                    arguments("byte", "128"),
                    arguments("int", "5.0"),
                    arguments("integer", "1.5"),
                    // Longer numbers are not typed.
                    arguments("integer", "1" + "0".repeat(1000)),
                    arguments("decimal", "1e99999"),
                    arguments("decimal", "1234567890123456789012345678901234567890"),
                    arguments("float", "1e50"),
                    arguments("float", "INF"),
                    arguments("double", "1e99999"),
                    arguments("double", "NaN"),
                    arguments("double", "Infinity"),
                    arguments("date", "2026-02-30"),
                    arguments("date", "2026-09-23Z"),
                    arguments("time", "12:30:45Z"),
                    arguments("dateTime", "2026-09-23"),
                    // Valid in XML Schema, but rejected by the Java parsers.
                    arguments("date", "10000-01-01"),
                    arguments("dateTime", "10000-01-01T00:00:00"),
                    arguments("time", "24:00:00"),
                    arguments("dateTime", "2026-09-23T24:00:00"),
                    arguments("base64Binary", "AQ="),
                    arguments("base64Binary", "@@@@"),
                    arguments("anyURI", "urn:x"),
                    arguments("Int", "5"));
        }

        /** Values that the Java parsers accept, but XML Schema doesn't. */
        Stream<Arguments> invalidLexicalForms() {
            return Stream.of(
                    arguments("float", "1f"),
                    arguments("float", "1.5F"),
                    arguments("float", "1d"),
                    arguments("double", "1.5D"),
                    arguments("float", "0x1.8p1"),
                    arguments("double", "0x1.8p1"),
                    // Digits from other scripts: Arabic-Indic three, and fullwidth one and two.
                    arguments("int", "\u0663"), // ٣
                    arguments("long", "\uff11\uff12"), // １２
                    arguments("integer", "\u0663"), // ٣
                    arguments("decimal", "\u0663"), // ٣
                    arguments("decimal", "1e5"),
                    arguments("time", "12:30"),
                    arguments("dateTime", "2026-09-23T12:30"),
                    arguments("dateTime", "2026-09-23t12:30:00"),
                    arguments("dateTime", "2026-09-23T12:30:00z"),
                    arguments("dateTime", "2026-09-23T12:30:00+01:00[Europe/Paris]"),
                    // Without padding, and with unused bits that aren't zero.
                    arguments("base64Binary", "AQI"),
                    arguments("base64Binary", "AR=="));
        }

        // The XML Schema validator of the JDK is the reference for which values are valid. It
        // implements XML Schema 1.0, so values that only 1.1 allows, like the year 0000, can't be
        // typed values.

        @ParameterizedTest(name = "{0}: {1}")
        @MethodSource("typedValues")
        void testTypedValuesAreValidInXmlSchema(String xsiType, String text) throws Exception {
            assertThat(isValidInXmlSchema(xsiType, text)).isTrue();
        }

        @ParameterizedTest(name = "{0}: {1}")
        @MethodSource("invalidLexicalForms")
        void testInvalidLexicalFormsAreInvalidInXmlSchema(String xsiType, String text)
                throws Exception {
            assertThat(isValidInXmlSchema(xsiType, text)).isFalse();
        }

        /** The validator checks the text against the xsi:type of the element, without a schema. */
        private boolean isValidInXmlSchema(String xsiType, String text) throws Exception {
            final String xml =
                    String.format(
                            "<a xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
                                    + " xmlns:xs=\"http://www.w3.org/2001/XMLSchema\""
                                    + " xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\""
                                    + " xsi:type=\"%s\">%s</a>",
                            xsiType.contains(":") ? xsiType : "xs:" + xsiType, text);
            try {
                SchemaFactory.newDefaultInstance()
                        .newSchema()
                        .newValidator()
                        .validate(new StreamSource(new StringReader(xml)));
                return true;
            } catch (SAXException e) {
                // Any other error, e.g. an unknown type, is a mistake in the test.
                if (!e.getMessage().startsWith("cvc-datatype-valid")) {
                    throw e;
                }
                return false;
            }
        }

        private Variant parseTyped(String xsiType, String text) {
            return parser.parse(String.format("<a xsi:type=\"%s\">%s</a>", xsiType, text), false)
                    .getField("a");
        }
    }

    /** The mapping with {@code force_array}. */
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class ForceArray {

        @ParameterizedTest(name = "{0}")
        @MethodSource("forceArrayMappings")
        void testForceArray(String xml, String expectedJson) {
            assertThat(parser.parse(xml, true).toJson()).isEqualTo(expectedJson);
        }

        Stream<Arguments> forceArrayMappings() {
            return Stream.of(
                    // The root element is never wrapped, since there is always exactly one.
                    arguments("<title>Dune</title>", "{\"title\":\"Dune\"}"),
                    arguments(
                            "<book><title>Dune</title></book>",
                            "{\"book\":{\"title\":[\"Dune\"]}}"),
                    arguments(
                            "<book pages=\"320\">Dune</book>",
                            "{\"book\":{\"$\":[\"Dune\"],\"@pages\":\"320\"}}"),
                    arguments(
                            "<book><title>Dune</title><author>A</author><author>B</author></book>",
                            "{\"book\":{\"#\":{\"author\":[1,2],\"title\":[0]},"
                                    + "\"author\":[\"A\",\"B\"],\"title\":[\"Dune\"]}}"),
                    arguments(
                            "<book><price xsi:type=\"int\">5</price></book>",
                            "{\"book\":{\"price\":[5]}}"),
                    arguments("<book><a xsi:nil=\"true\"/></book>", "{\"book\":{\"a\":[null]}}"),
                    arguments(
                            "<book><a xsi:nil=\"true\" id=\"1\"/></book>",
                            "{\"book\":{\"a\":[{\"$\":[null],\"@id\":\"1\"}]}}"));
        }
    }

    /** Documents that fail to parse. */
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class InvalidAndUnsafeInput {

        // The messages are localized. They are checked in English, the locale of the test JVM.
        @ParameterizedTest(name = "[{index}] {0}")
        @MethodSource("invalidDocuments")
        void testInvalidXml(String xml, String expectedMessage) {
            assertThatThrownBy(() -> parser.parse(xml, false))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(expectedMessage);
        }

        Stream<Arguments> invalidDocuments() {
            return Stream.of(
                    arguments("", "Premature end of file."),
                    arguments("   ", "Premature end of file."),
                    arguments("text", "Content is not allowed in prolog."),
                    arguments(
                            "<a>",
                            "XML document structures must start and end within the same entity."),
                    arguments(
                            "<a></b>",
                            "The element type \"a\" must be terminated by the matching end-tag \"</a>\"."),
                    arguments("<a/>trailing", "Content is not allowed in trailing section."),
                    arguments(
                            "<a/><b/>",
                            "The markup in the document following the root element must be well-formed."),
                    arguments(
                            "<a b=\"1\" b=\"2\"/>",
                            "Attribute \"b\" was already specified for element \"a\"."),
                    arguments(
                            "<a>&undefined;</a>",
                            "The entity \"undefined\" was referenced, but not declared."),
                    // XML 1.0 doesn't allow control characters, not even as a reference.
                    arguments(
                            "<a>&#1;</a>",
                            "Character reference \"&#1\" is an invalid XML character."),
                    arguments("{\"a\": 1}", "Content is not allowed in prolog."));
        }

        @Test
        void testXml11IsRejected() {
            assertThatThrownBy(() -> parser.parse("<?xml version=\"1.1\"?><a>x&#1;</a>", false))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("XML 1.1 documents are not supported");
        }

        @ParameterizedTest
        @ValueSource(
                strings = {
                    // External entity
                    "<!DOCTYPE a [<!ENTITY e SYSTEM \"file:///etc/hosts\">]><a>&e;</a>",
                    "<!DOCTYPE a [<!ENTITY e SYSTEM \"http://localhost/e\">]><a>&e;</a>",
                    // External parameter entity
                    "<!DOCTYPE a [<!ENTITY % p SYSTEM \"file:///etc/hosts\"> %p;]><a/>",
                    // External DTD
                    "<!DOCTYPE a SYSTEM \"file:///etc/hosts\"><a/>",
                    "<!DOCTYPE a PUBLIC \"-//A//DTD A//EN\" \"http://localhost/a.dtd\"><a/>"
                })
        void testExternalResourcesAreRefused(String xml) {
            assertThatThrownBy(() -> parser.parse(xml, false))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("External entities and DTDs are not supported");
        }

        @Test
        void testUnusedExternalEntityDeclaration() {
            assertThat(
                            parser.parse(
                                            "<!DOCTYPE a [<!ENTITY e SYSTEM \"file:///etc/hosts\">]>"
                                                    + "<a>x</a>",
                                            false)
                                    .toJson())
                    .isEqualTo("{\"a\":\"x\"}");
        }

        // The limits are checked by their JAXP error codes, since the messages are localized.

        @Test
        void testNestingDepthIsLimited() {
            assertThatNoException().isThrownBy(() -> parser.parse(nested(500), true));
            assertThatThrownBy(() -> parser.parse(nested(501), false))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("JAXP00010006");
        }

        @Test
        void testNumberOfEntityExpansionsIsLimited() {
            // The billion laughs attack, see https://en.wikipedia.org/wiki/Billion_laughs_attack.
            // Each entity references the previous one ten times, so &e9; expands to 10^9 "lol"s.
            final StringBuilder xml = new StringBuilder("<!DOCTYPE a [<!ENTITY e0 \"lol\">");
            for (int i = 1; i <= 9; i++) {
                xml.append(String.format("<!ENTITY e%d \"", i));
                for (int j = 0; j < 10; j++) {
                    xml.append(String.format("&e%d;", i - 1));
                }
                xml.append("\">");
            }
            xml.append("]><a>&e9;</a>");

            assertThatThrownBy(() -> parser.parse(xml.toString(), false))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("JAXP00010001");
        }

        @Test
        void testSizeOfExpandedEntitiesIsLimited() {
            // About 50 million characters from about 50,000 expansions.
            final String xml =
                    "<!DOCTYPE a [<!ENTITY a \""
                            + "x".repeat(1000)
                            + "\"><!ENTITY b \""
                            + "&a;".repeat(50)
                            + "\">]><a>"
                            + "&b;".repeat(990)
                            + "</a>";

            assertThatThrownBy(() -> parser.parse(xml, false))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("JAXP00010004");
        }

        private String nested(int depth) {
            return "<a>".repeat(depth) + "</a>".repeat(depth);
        }
    }
}
