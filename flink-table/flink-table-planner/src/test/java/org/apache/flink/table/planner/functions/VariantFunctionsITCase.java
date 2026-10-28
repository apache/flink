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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.time.Instant;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.DataTypes.VARIANT;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.jsonString;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.nullOf;

/**
 * Tests for the VARIANT functions that are not covered by the JSON and XML function tests.
 *
 * <p>Path syntax is covered by {@code SimplePathTest}. A path that changes from row to row is
 * covered by {@code VariantSemanticTest}.
 */
class VariantFunctionsITCase extends BuiltInFunctionTestBase {

    private static final String ORDER =
            "{\"order_id\": 17, \"user\": {\"name\": \"ada\", \"email\": \"ada@example.com\"}, "
                    + "\"items\": [{\"sku\": \"A1\", \"qty\": 2}], \"coupon\": null, "
                    + "\"price\": \"12.50\"}";

    @Override
    Configuration getConfiguration() {
        return super.getConfiguration().set(TableConfigOptions.LOCAL_TIME_ZONE, "Asia/Shanghai");
    }

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(variantGet(), variantGetFailure(), tryVariantGet());
    }

    private static TestSetSpec variantGet() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.VARIANT_GET)
                .onFieldsWithData(ORDER, "$.user.name", null, Instant.EPOCH)
                .andDataTypes(STRING().notNull(), STRING().notNull(), STRING(), TIMESTAMP_LTZ(3))
                .testResult(
                        $("f0").parseJson().variantGet("$.order_id", INT()),
                        "VARIANT_GET(PARSE_JSON(f0), '$.order_id', 'int')",
                        17,
                        INT())
                .testResult(
                        jsonString($("f0").parseJson().variantGet("$.user.name", VARIANT())),
                        "JSON_STRING(VARIANT_GET(PARSE_JSON(f0), '$.user.name', 'VARIANT'))",
                        "\"ada\"",
                        STRING())
                // A path that points at nothing gives NULL, so even a NOT NULL type is nullable.
                .testResult(
                        $("f0").parseJson().variantGet("$.missing", INT().notNull()),
                        "VARIANT_GET(PARSE_JSON(f0), '$.missing', 'INT NOT NULL')",
                        null,
                        INT())
                // A variant null stays a VARIANT, and casts to NULL.
                .testResult(
                        jsonString($("f0").parseJson().variantGet("$.coupon")),
                        "JSON_STRING(VARIANT_GET(PARSE_JSON(f0), '$.coupon'))",
                        "null",
                        STRING())
                .testResult(
                        $("f0").parseJson().variantGet("$.coupon", STRING()),
                        "VARIANT_GET(PARSE_JSON(f0), '$.coupon', 'STRING')",
                        null,
                        STRING())
                .testResult(
                        $("f0").parseJson().variantGet($("f1"), STRING()),
                        "VARIANT_GET(PARSE_JSON(f0), f1, 'STRING')",
                        "ada",
                        STRING())
                .testResult(
                        $("f2").parseJson().variantGet("$.order_id", INT()),
                        "VARIANT_GET(PARSE_JSON(f2), '$.order_id', 'INT')",
                        null,
                        INT())
                .testResult(
                        $("f0").parseJson().variantGet(nullOf(STRING())),
                        "VARIANT_GET(PARSE_JSON(f0), NULL)",
                        null,
                        VARIANT())
                // A TIMESTAMP_LTZ renders in the session time zone, as in CAST.
                .testResult(
                        $("f3").cast(VARIANT()).variantGet("$", STRING()),
                        "VARIANT_GET(CAST(f3 AS VARIANT), '$', 'STRING')",
                        "1970-01-01 08:00:00.000000",
                        STRING());
    }

    private static TestSetSpec variantGetFailure() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.VARIANT_GET, "failures")
                .onFieldsWithData(ORDER)
                .andDataTypes(STRING().notNull())
                .testSqlRuntimeError(
                        "VARIANT_GET(PARSE_JSON(f0), '$.price', 'DECIMAL(4, 2)')",
                        TableRuntimeException.class,
                        "Failed to cast the VARIANT value at path '$.price' to DECIMAL(4, 2): "
                                + "Cannot cast a VARIANT STRING value to DECIMAL(4, 2).")
                // A path that becomes a literal only during optimization fails at run time.
                .testTableApiRuntimeError(
                        $("f0").parseJson().variantGet(lit("$.user[").cast(STRING())),
                        TableRuntimeException.class,
                        "Invalid path '$.user['")
                .testSqlValidationError(
                        "VARIANT_GET(PARSE_JSON(f0), '$.items[-1]')",
                        "Invalid path '$.items[-1]': expected an index or a quoted name after "
                                + "'$.items['.")
                .testSqlValidationError(
                        "VARIANT_GET(PARSE_JSON(f0), '$.a', 'MULTISET<INT>')",
                        "VARIANT_GET cannot return type MULTISET<INT>, because a VARIANT cannot "
                                + "be cast to it.")
                .testSqlValidationError(
                        "VARIANT_GET(PARSE_JSON(f0), '$.a', 'NOT A TYPE')",
                        "Invalid type 'NOT A TYPE' for VARIANT_GET:")
                .testSqlValidationError(
                        "VARIANT_GET(PARSE_JSON(f0), '$.a', f0)",
                        "Invalid input arguments. Expected signatures are:");
    }

    private static TestSetSpec tryVariantGet() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.TRY_VARIANT_GET)
                .onFieldsWithData(ORDER)
                .andDataTypes(STRING().notNull())
                .testResult(
                        $("f0").parseJson().tryVariantGet("$.price", DECIMAL(4, 2)),
                        "TRY_VARIANT_GET(PARSE_JSON(f0), '$.price', 'DECIMAL(4, 2)')",
                        null,
                        DECIMAL(4, 2))
                // A path that becomes a literal only during optimization gives NULL.
                .testResult(
                        $("f0").parseJson().tryVariantGet(lit("$.user[").cast(STRING()), STRING()),
                        "TRY_VARIANT_GET(PARSE_JSON(f0), CAST('$.user[' AS STRING), 'STRING')",
                        null,
                        STRING())
                // A malformed literal path still fails at planning time.
                .testTableApiValidationError(
                        $("f0").parseJson().tryVariantGet("$.user["),
                        "Invalid path '$.user[': expected an index or a quoted name after "
                                + "'$.user['.");
    }
}
