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

package org.apache.flink.table.expressions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.JsonExistsOnError;
import org.apache.flink.table.api.JsonOnNull;
import org.apache.flink.table.api.JsonQueryOnEmptyOrError;
import org.apache.flink.table.api.JsonQueryWrapper;
import org.apache.flink.table.api.JsonType;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.operations.ValuesQueryOperation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.table.utils.FunctionLookupMock;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.timestampDiff;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for serializing {@link BuiltInFunctionDefinitions} into a SQL string. */
public class ExpressionSerializationTest {

    public static Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forExpr(Expressions.uuid()).expectStr("UUID()"),
                TestSpec.forExpr($("f0").abs())
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("ABS(`f0`)"),
                TestSpec.forExpr($("f0").isLess(123))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("`f0` < 123"),
                TestSpec.forExpr($("f0").isLessOrEqual(123))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("`f0` <= 123"),
                TestSpec.forExpr($("f0").isEqual(123))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("`f0` = 123"),
                TestSpec.forExpr($("f0").isNotEqual(123))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("`f0` <> 123"),
                TestSpec.forExpr($("f0").isGreaterOrEqual(123))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("`f0` >= 123"),
                TestSpec.forExpr($("f0").isGreater(123))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("`f0` > 123"),
                TestSpec.forExpr($("f0").isNull())
                        .withField("f0", DataTypes.BOOLEAN())
                        .expectStr("`f0` IS NULL"),
                TestSpec.forExpr($("f0").isNotNull())
                        .withField("f0", DataTypes.BOOLEAN())
                        .expectStr("`f0` IS NOT NULL"),
                TestSpec.forExpr($("f0").isTrue())
                        .withField("f0", DataTypes.BOOLEAN())
                        .expectStr("`f0` IS TRUE"),
                TestSpec.forExpr($("f0").isNotTrue())
                        .withField("f0", DataTypes.BOOLEAN())
                        .expectStr("`f0` IS NOT TRUE"),
                TestSpec.forExpr($("f0").isFalse())
                        .withField("f0", DataTypes.BOOLEAN())
                        .expectStr("`f0` IS FALSE"),
                TestSpec.forExpr($("f0").isNotFalse())
                        .withField("f0", DataTypes.BOOLEAN())
                        .expectStr("`f0` IS NOT FALSE"),
                TestSpec.forExpr($("f0").not())
                        .withField("f0", DataTypes.BOOLEAN())
                        .expectStr("NOT `f0`"),
                TestSpec.forExpr(
                                Expressions.and(
                                        $("f0").isNotNull(),
                                        $("f0").isLess(420),
                                        $("f0").isGreater(123)))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("(`f0` IS NOT NULL) AND (`f0` < 420) AND (`f0` > 123)"),
                TestSpec.forExpr(
                                Expressions.or(
                                        $("f0").isNotNull(),
                                        $("f0").isLess(420),
                                        $("f0").isGreater(123)))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("(`f0` IS NOT NULL) OR (`f0` < 420) OR (`f0` > 123)"),
                TestSpec.forExpr(
                                Expressions.ifThenElse(
                                        $("f0").isNotNull(), $("f0").plus(420), $("f0").minus(123)))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr(
                                "CASE WHEN `f0` IS NOT NULL THEN `f0` + 420 ELSE `f0` - 123 END"),
                TestSpec.forExpr($("f0").times(3).dividedBy($("f1")))
                        .withField("f0", DataTypes.BIGINT())
                        .withField("f1", DataTypes.BIGINT())
                        .expectStr("(`f0` * 3) / `f1`"),
                TestSpec.forExpr($("f0").mod(5))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("`f0` % 5"),
                TestSpec.forExpr(Expressions.negative($("f0")))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("- `f0`"),
                TestSpec.forExpr(Expressions.randInteger(Expressions.lit(10)))
                        .expectStr("RAND_INTEGER(10)"),
                TestSpec.forExpr($("f0").in(1, 2, 3, 4, 5))
                        .withField("f0", DataTypes.INT())
                        .expectStr("`f0` IN (1, 2, 3, 4, 5)"),
                TestSpec.forExpr($("f0").cast(DataTypes.SMALLINT()))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("CAST(`f0` AS SMALLINT)"),
                TestSpec.forExpr($("f0").tryCast(DataTypes.SMALLINT()))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("TRY_CAST(`f0` AS SMALLINT)"),
                TestSpec.forExpr(Expressions.array($("f0"), $("f1"), "ABC"))
                        .withField("f0", DataTypes.STRING())
                        .withField("f1", DataTypes.STRING())
                        .expectStr("ARRAY[`f0`, `f1`, 'ABC']"),
                TestSpec.forExpr(Expressions.map($("f0"), $("f1"), "ABC", "DEF"))
                        .withField("f0", DataTypes.STRING())
                        .withField("f1", DataTypes.STRING())
                        .expectStr("MAP[`f0`, `f1`, 'ABC', 'DEF']"),
                TestSpec.forExpr($("f0").at(2))
                        .withField("f0", DataTypes.ARRAY(DataTypes.STRING()))
                        .expectStr("`f0`[2]"),
                TestSpec.forExpr($("f0").at("abc"))
                        .withField("f0", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
                        .expectStr("`f0`['abc']"),
                TestSpec.forExpr($("f0").between(1, 10))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("`f0` BETWEEN 1 AND 10"),
                TestSpec.forExpr($("f0").notBetween(1, 10))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("`f0` NOT BETWEEN 1 AND 10"),
                TestSpec.forExpr($("f0").like("ABC"))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("`f0` LIKE 'ABC'"),
                TestSpec.forExpr($("f0").similar("ABC"))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("`f0` SIMILAR TO 'ABC'"),
                TestSpec.forExpr($("f0").position("ABC"))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("POSITION(`f0` IN 'ABC')"),
                TestSpec.forExpr($("f0").trim("ABC"))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("TRIM BOTH 'ABC' FROM `f0`"),
                TestSpec.forExpr($("f0").trimLeading("ABC"))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("TRIM LEADING 'ABC' FROM `f0`"),
                TestSpec.forExpr($("f0").trimTrailing("ABC"))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("TRIM TRAILING 'ABC' FROM `f0`"),
                TestSpec.forExpr($("f0").overlay("ABC", 2))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("OVERLAY(`f0` PLACING 'ABC' FROM 2)"),
                TestSpec.forExpr($("f0").overlay("ABC", 2, 5))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("OVERLAY(`f0` PLACING 'ABC' FROM 2 FOR 5)"),
                TestSpec.forExpr($("f0").substr(2))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("SUBSTR(`f0`, 2)"),
                TestSpec.forExpr($("f0").substr(2, 5))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("SUBSTR(`f0`, 2, 5)"),
                TestSpec.forExpr($("f0").substring(2))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("SUBSTRING(`f0` FROM 2)"),
                TestSpec.forExpr($("f0").substring(2, 5))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("SUBSTRING(`f0` FROM 2 FOR 5)"),
                TestSpec.forExpr($("f0").charLength())
                        .withField("f0", DataTypes.STRING())
                        .expectStr("CHAR_LENGTH(`f0`)"),
                TestSpec.forExpr($("f0").fromBase64())
                        .withField("f0", DataTypes.STRING())
                        .expectStr("FROM_BASE64(`f0`)"),
                TestSpec.forExpr($("f0").toBase64())
                        .withField("f0", DataTypes.STRING())
                        .expectStr("TO_BASE64(`f0`)"),
                TestSpec.forExpr($("f0").parseUrl(lit("HOST")))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("PARSE_URL(`f0`, 'HOST')"),
                TestSpec.forExpr($("f0").regexpReplace(lit("[0-9]"), lit("$")))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("REGEXP_REPLACE(`f0`, '[0-9]', '$')"),
                TestSpec.forExpr($("f0").splitIndex(lit(":"), lit(2)))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("SPLIT_INDEX(`f0`, ':', 2)"),
                TestSpec.forExpr($("f0").strToMap())
                        .withField("f0", DataTypes.STRING())
                        .expectStr("STR_TO_MAP(`f0`)"),
                TestSpec.forExpr($("f0").get("g0").plus($("f0").get("g1").get("h1")))
                        .withField(
                                "f0",
                                DataTypes.ROW(
                                        DataTypes.FIELD("g0", DataTypes.BIGINT()),
                                        DataTypes.FIELD(
                                                "g1",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "h1", DataTypes.BIGINT())))))
                        .expectStr("(`f0`.`g0`) + (`f0`.`g1`.`h1`)"),
                TestSpec.forExpr($("f0").abs().as("absolute`F0"))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("(ABS(`f0`)) AS `absolute``F0`"),

                // JSON functions
                TestSpec.forExpr($("f0").isJson())
                        .withField("f0", DataTypes.STRING())
                        .expectStr("`f0` IS JSON"),
                TestSpec.forExpr($("f0").isJson(JsonType.SCALAR))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("`f0` IS JSON SCALAR"),
                TestSpec.forExpr($("f0").jsonExists("$.a"))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("JSON_EXISTS(`f0`, '$.a')"),
                TestSpec.forExpr($("f0").jsonExists("$.a", JsonExistsOnError.UNKNOWN))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("JSON_EXISTS(`f0`, '$.a' UNKNOWN ON ERROR)"),
                TestSpec.forExpr($("f0").jsonValue("$.a"))
                        .withField("f0", DataTypes.STRING())
                        .expectStr(
                                "JSON_VALUE(`f0`, '$.a' RETURNING VARCHAR(2147483647) NULL ON EMPTY NULL ON ERROR)"),
                TestSpec.forExpr($("f0").jsonValue("$.a", DataTypes.BOOLEAN(), false))
                        .withField("f0", DataTypes.STRING())
                        .expectStr(
                                "JSON_VALUE(`f0`, '$.a' RETURNING BOOLEAN DEFAULT FALSE ON EMPTY DEFAULT FALSE ON ERROR)"),
                TestSpec.forExpr(
                                $("f0").jsonValue(
                                                "$.a",
                                                DataTypes.BIGINT(),
                                                JsonValueOnEmptyOrError.DEFAULT,
                                                1,
                                                JsonValueOnEmptyOrError.ERROR,
                                                null))
                        .withField("f0", DataTypes.STRING())
                        .expectStr(
                                "JSON_VALUE(`f0`, '$.a' RETURNING BIGINT DEFAULT 1 ON EMPTY ERROR ON ERROR)"),
                TestSpec.forExpr($("f0").jsonQuery("$.a"))
                        .withField("f0", DataTypes.STRING())
                        .expectStr(
                                "JSON_QUERY(`f0`, '$.a' RETURNING VARCHAR(2147483647) WITHOUT ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)"),
                TestSpec.forExpr($("f0").jsonQuery("$.a", JsonQueryWrapper.UNCONDITIONAL_ARRAY))
                        .withField("f0", DataTypes.STRING())
                        .expectStr(
                                "JSON_QUERY(`f0`, '$.a' RETURNING VARCHAR(2147483647) WITH UNCONDITIONAL ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)"),
                TestSpec.forExpr(
                                $("f0").jsonQuery(
                                                "$.a",
                                                JsonQueryWrapper.CONDITIONAL_ARRAY,
                                                JsonQueryOnEmptyOrError.EMPTY_OBJECT,
                                                JsonQueryOnEmptyOrError.EMPTY_ARRAY))
                        .withField("f0", DataTypes.STRING())
                        .expectStr(
                                "JSON_QUERY(`f0`, '$.a' RETURNING VARCHAR(2147483647) WITH CONDITIONAL ARRAY WRAPPER EMPTY OBJECT ON EMPTY EMPTY ARRAY ON ERROR)"),
                TestSpec.forExpr(
                                Expressions.jsonObject(JsonOnNull.ABSENT, "k1", $("f0"), "k2", 123))
                        .withField("f0", DataTypes.STRING())
                        .expectStr(
                                "JSON_OBJECT(KEY 'k1' VALUE `f0`, KEY 'k2' VALUE 123 ABSENT ON NULL)"),
                TestSpec.forExpr(Expressions.jsonArray(JsonOnNull.ABSENT, "k1", $("f0"), "k2"))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("JSON_ARRAY('k1', `f0`, 'k2' ABSENT ON NULL)"),
                TestSpec.forExpr(Expressions.jsonArrayAgg(JsonOnNull.ABSENT, $("f0")))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("JSON_ARRAYAGG(`f0` ABSENT ON NULL)"),
                TestSpec.forExpr(Expressions.jsonArrayAgg(JsonOnNull.NULL, $("f0")))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("JSON_ARRAYAGG(`f0` NULL ON NULL)"),
                TestSpec.forExpr(Expressions.jsonObjectAgg(JsonOnNull.ABSENT, $("f0"), $("f1")))
                        .withField("f0", DataTypes.STRING())
                        .withField("f1", DataTypes.STRING())
                        .expectStr("JSON_OBJECTAGG(KEY `f0` VALUE `f1` ABSENT ON NULL)"),
                TestSpec.forExpr(Expressions.jsonObjectAgg(JsonOnNull.NULL, $("f0"), $("f1")))
                        .withField("f0", DataTypes.STRING())
                        .withField("f1", DataTypes.STRING())
                        .expectStr("JSON_OBJECTAGG(KEY `f0` VALUE `f1` NULL ON NULL)"),

                // Aggregate functions
                TestSpec.forExpr(
                                $("f0").count()
                                        .distinct()
                                        .plus($("f0").avg().distinct())
                                        .plus($("f0").max()))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("((COUNT(DISTINCT `f0`)) + (AVG(DISTINCT `f0`))) + (MAX(`f0`))"),
                TestSpec.forExpr($("f0").stddevPop())
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("STDDEV_POP(`f0`)"),
                TestSpec.forExpr($("f0").stddevSamp())
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("STDDEV_SAMP(`f0`)"),
                TestSpec.forExpr($("f0").varPop())
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("VAR_POP(`f0`)"),
                TestSpec.forExpr($("f0").varSamp())
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("VAR_SAMP(`f0`)"),

                // Time functions
                TestSpec.forExpr($("f0").extract(TimeIntervalUnit.HOUR))
                        .withField("f0", DataTypes.TIMESTAMP())
                        .expectStr("EXTRACT(HOUR FROM `f0`)"),
                TestSpec.forExpr($("f0").floor(TimeIntervalUnit.HOUR))
                        .withField("f0", DataTypes.TIMESTAMP())
                        .expectStr("FLOOR(`f0` TO HOUR)"),
                TestSpec.forExpr($("f0").ceil(TimeIntervalUnit.HOUR))
                        .withField("f0", DataTypes.TIMESTAMP())
                        .expectStr("CEIL(`f0` TO HOUR)"),
                TestSpec.forExpr(
                                Expressions.temporalOverlaps(
                                        $("f0"), $("f1"),
                                        $("f2"), $("f3")))
                        .withField("f0", DataTypes.TIMESTAMP())
                        .withField("f1", DataTypes.TIMESTAMP())
                        .withField("f2", DataTypes.TIMESTAMP())
                        .withField("f3", DataTypes.TIMESTAMP())
                        .expectStr("(`f0`, `f1`) OVERLAPS (`f2`, `f3`)"),
                TestSpec.forExpr(timestampDiff(TimePointUnit.DAY, $("f0"), $("f1")))
                        .withField("f0", DataTypes.TIMESTAMP())
                        .withField("f1", DataTypes.TIMESTAMP())
                        .expectStr("TIMESTAMPDIFF(DAY, `f0`, `f1`)"),
                TestSpec.forExpr(Expressions.currentDate()).expectStr("CURRENT_DATE()"),
                TestSpec.forExpr(Expressions.currentTime()).expectStr("CURRENT_TIME()"),
                TestSpec.forExpr(Expressions.currentTimestamp()).expectStr("CURRENT_TIMESTAMP()"),
                TestSpec.forExpr(Expressions.dateFormat($("f0"), lit("yyyy-MM-dd")))
                        .withField("f0", DataTypes.TIMESTAMP(3))
                        .expectStr("DATE_FORMAT(`f0`, 'yyyy-MM-dd')"),
                TestSpec.forExpr(Expressions.toTimestamp($("f0")))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("TO_TIMESTAMP(`f0`)"),
                TestSpec.forExpr(Expressions.toTimestampLtz($("f0"), lit(3)))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("TO_TIMESTAMP_LTZ(`f0`, 3)"),
                TestSpec.forExpr($("f0").toDate())
                        .withField("f0", DataTypes.STRING())
                        .expectStr("CAST(`f0` AS DATE)"),
                TestSpec.forExpr($("f0").toTime())
                        .withField("f0", DataTypes.STRING())
                        .expectStr("CAST(`f0` AS TIME(0))"),
                TestSpec.forExpr($("f0").toTimestamp())
                        .withField("f0", DataTypes.STRING())
                        .expectStr("CAST(`f0` AS TIMESTAMP(3))"),
                TestSpec.forExpr(Expressions.convertTz($("f0"), lit("PST"), lit("GMT")))
                        .withField("f0", DataTypes.STRING())
                        .expectStr("CONVERT_TZ(`f0`, 'PST', 'GMT')"),
                TestSpec.forExpr(Expressions.fromUnixtime($("f0")))
                        .withField("f0", DataTypes.BIGINT())
                        .expectStr("FROM_UNIXTIME(`f0`)"),
                TestSpec.forExpr(Expressions.unixTimestamp()).expectStr("UNIX_TIMESTAMP()"));
    }

    @ParameterizedTest
    @MethodSource("testData")
    void testSerialization(TestSpec spec) {
        final List<ResolvedExpression> resolved =
                ExpressionResolver.resolverFor(
                                TableConfig.getDefault(),
                                Thread.currentThread().getContextClassLoader(),
                                name -> Optional.empty(),
                                new FunctionLookupMock(Collections.emptyMap()),
                                new DataTypeFactoryMock(),
                                (sqlExpression, inputRowType, outputType) -> null,
                                new ValuesQueryOperation(
                                        Collections.emptyList(),
                                        ResolvedSchema.of(new ArrayList<>(spec.columns.values()))))
                        .build()
                        .resolve(Collections.singletonList(spec.expr));

        assertThat(resolved)
                .hasSize(1)
                .extracting(ResolvedExpression::asSerializableString)
                .containsOnly(spec.expectedStr);
    }

    private static class TestSpec {
        private final Expression expr;
        private String expectedStr;

        private final Map<String, Column> columns = new HashMap<>();

        public TestSpec(Expression expr) {
            this.expr = expr;
        }

        public static TestSpec forExpr(Expression expr) {
            return new TestSpec(expr);
        }

        public TestSpec withField(String name, DataType dataType) {
            this.columns.put(name, Column.physical(name, dataType));
            return this;
        }

        public TestSpec expectStr(String expected) {
            this.expectedStr = expected;
            return this;
        }

        @Override
        public String toString() {
            return expr.asSummaryString();
        }
    }
}
