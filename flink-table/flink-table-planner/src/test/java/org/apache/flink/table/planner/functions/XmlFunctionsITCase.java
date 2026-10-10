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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.jsonString;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.nullOf;

/**
 * Tests for {@link BuiltInFunctionDefinitions#PARSE_XML} and {@link
 * BuiltInFunctionDefinitions#TRY_PARSE_XML}.
 *
 * <p>The bulk of the XML mapping is covered by {@code XmlToVariantParserTest}.
 */
class XmlFunctionsITCase extends BuiltInFunctionTestBase {

    private static final String BOOK =
            "<book pages=\"320\">\n"
                    + "  <title>Dune</title>\n"
                    + "  <price xsi:type=\"decimal\">12.50</price>\n"
                    + "</book>";

    private static final String BOOK_JSON =
            "{\"book\":{\"#\":{\"price\":1,\"title\":0},"
                    + "\"@pages\":\"320\",\"price\":12.5,\"title\":\"Dune\"}}";

    private static final String BOOK_FORCE_ARRAY_JSON =
            "{\"book\":{\"#\":{\"price\":[1],\"title\":[0]},"
                    + "\"@pages\":\"320\",\"price\":[12.5],\"title\":[\"Dune\"]}}";

    private static final String INVALID = "<book>";

    private static final String EXTERNAL_ENTITY =
            "<!DOCTYPE a [<!ENTITY e SYSTEM \"file:///etc/hosts\">]><a>&e;</a>";

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(parseXmlSpec(), tryParseXmlSpec(), constantFoldingSpec());
    }

    private static TestSetSpec parseXmlSpec() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.PARSE_XML)
                .onFieldsWithData(BOOK, INVALID, EXTERNAL_ENTITY, true, null)
                .andDataTypes(
                        STRING().notNull(),
                        STRING().notNull(),
                        STRING().notNull(),
                        BOOLEAN().notNull(),
                        BOOLEAN())
                .testResult(
                        jsonString($("f0").parseXml()),
                        "JSON_STRING(PARSE_XML(f0))",
                        BOOK_JSON,
                        STRING().notNull())
                .testResult(
                        jsonString($("f0").parseXml(false)),
                        "JSON_STRING(PARSE_XML(f0, FALSE))",
                        BOOK_JSON,
                        STRING().notNull())
                .testResult(
                        jsonString($("f0").parseXml(true)),
                        "JSON_STRING(PARSE_XML(f0, TRUE))",
                        BOOK_FORCE_ARRAY_JSON,
                        STRING().notNull())
                .testResult(
                        jsonString(call("PARSE_XML", $("f0"), $("f3"))),
                        "JSON_STRING(PARSE_XML(f0, f3))",
                        BOOK_FORCE_ARRAY_JSON,
                        STRING().notNull())
                // A NULL force_array is treated as false.
                .testResult(
                        jsonString(call("PARSE_XML", $("f0"), $("f4"))),
                        "JSON_STRING(PARSE_XML(f0, f4))",
                        BOOK_JSON,
                        STRING().notNull())
                .testResult(
                        $("f0").parseXml().at("book").at("title").cast(STRING()),
                        "CAST(PARSE_XML(f0).book.title AS STRING)",
                        "Dune",
                        STRING())
                // Untyped values are strings, typed values cast directly.
                .testResult(
                        $("f0").parseXml().at("book").at("@pages").cast(STRING()).cast(INT()),
                        "CAST(CAST(PARSE_XML(f0)['book']['@pages'] AS STRING) AS INT)",
                        320,
                        INT())
                .testResult(
                        $("f0").parseXml().at("book").at("price").cast(DECIMAL(4, 2)),
                        "CAST(PARSE_XML(f0)['book']['price'] AS DECIMAL(4, 2))",
                        new BigDecimal("12.50"),
                        DECIMAL(4, 2))
                .testResult(
                        $("f0").parseXml(true).at("book").at("title").at(1).cast(STRING()),
                        "CAST(PARSE_XML(f0, TRUE)['book']['title'][1] AS STRING)",
                        "Dune",
                        STRING())
                .testResult(
                        jsonString(nullOf(STRING()).parseXml()),
                        "JSON_STRING(PARSE_XML(CAST(NULL AS STRING)))",
                        null,
                        STRING().nullable())
                .testSqlRuntimeError(
                        "PARSE_XML(f1)", TableRuntimeException.class, "Failed to parse XML string")
                .testTableApiRuntimeError(
                        $("f1").parseXml(),
                        TableRuntimeException.class,
                        "Failed to parse XML string")
                .testSqlRuntimeError(
                        "PARSE_XML(f2)", TableRuntimeException.class, "Failed to parse XML string")
                .testTableApiRuntimeError(
                        $("f2").parseXml(true),
                        TableRuntimeException.class,
                        "Failed to parse XML string")
                // FROM_BASE64 doesn't validate the decoded bytes, so this is <a>, 0xFF, </a>. An
                // invalid UTF-8 byte is read as U+FFFD, like in any other conversion to a string.
                .testSqlResult(
                        "JSON_STRING(PARSE_XML(FROM_BASE64('PGE+/zwvYT4=')))",
                        "{\"a\":\"\ufffd\"}",
                        STRING().notNull());
    }

    private static TestSetSpec tryParseXmlSpec() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.TRY_PARSE_XML)
                .onFieldsWithData(BOOK, INVALID, EXTERNAL_ENTITY, true)
                .andDataTypes(
                        STRING().notNull(),
                        STRING().notNull(),
                        STRING().notNull(),
                        BOOLEAN().notNull())
                .testResult(
                        jsonString($("f0").tryParseXml()),
                        "JSON_STRING(TRY_PARSE_XML(f0))",
                        BOOK_JSON,
                        STRING())
                .testResult(
                        jsonString($("f0").tryParseXml(true)),
                        "JSON_STRING(TRY_PARSE_XML(f0, TRUE))",
                        BOOK_FORCE_ARRAY_JSON,
                        STRING())
                .testResult(
                        jsonString(call("TRY_PARSE_XML", $("f0"), $("f3"))),
                        "JSON_STRING(TRY_PARSE_XML(f0, f3))",
                        BOOK_FORCE_ARRAY_JSON,
                        STRING())
                .testResult(
                        jsonString($("f1").tryParseXml()),
                        "JSON_STRING(TRY_PARSE_XML(f1))",
                        null,
                        STRING())
                .testResult(
                        jsonString($("f2").tryParseXml(false)),
                        "JSON_STRING(TRY_PARSE_XML(f2, FALSE))",
                        null,
                        STRING())
                .testResult(
                        jsonString(nullOf(STRING()).tryParseXml()),
                        "JSON_STRING(TRY_PARSE_XML(CAST(NULL AS STRING)))",
                        null,
                        STRING());
    }

    private static TestSetSpec constantFoldingSpec() {
        return TestSetSpec.forFunction(
                        BuiltInFunctionDefinitions.PARSE_XML, "Constant-folded literal input")
                .onFieldsWithData(1)
                .andDataTypes(INT())
                .withConstantFoldingEnabled()
                .testResult(
                        jsonString(lit("<a>x</a>").parseXml()),
                        "JSON_STRING(PARSE_XML('<a>x</a>'))",
                        "{\"a\":\"x\"}",
                        STRING().notNull())
                .testResult(
                        jsonString(lit("<a>").tryParseXml()),
                        "JSON_STRING(TRY_PARSE_XML('<a>'))",
                        null,
                        STRING());
    }
}
