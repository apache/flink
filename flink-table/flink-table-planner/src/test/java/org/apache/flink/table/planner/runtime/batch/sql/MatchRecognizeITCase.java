/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * imitations under the License.
 */

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvg;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.apache.calcite.sql.SqlMatchRecognize;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.api.common.typeinfo.Types.DOUBLE;
import static org.apache.flink.api.common.typeinfo.Types.INT;
import static org.apache.flink.api.common.typeinfo.Types.LONG;
import static org.apache.flink.api.common.typeinfo.Types.ROW_NAMED;
import static org.apache.flink.api.common.typeinfo.Types.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** IT Case for testing {@link SqlMatchRecognize}. */
public class MatchRecognizeITCase {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, false);
    }

    @Test
    public void testSimplePattern() {
        tEnv.createTemporaryView(
                "MyTable",
                tEnv.fromDataStream(
                        env.fromElements(
                                        Row.of(1, "a"),
                                        Row.of(2, "z"),
                                        Row.of(3, "b"),
                                        Row.of(4, "c"),
                                        Row.of(5, "d"),
                                        Row.of(6, "a"),
                                        Row.of(7, "b"),
                                        Row.of(8, "c"),
                                        Row.of(9, "h"))
                                .returns(ROW_NAMED(new String[] {"id", "name"}, INT, STRING)),
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .columnByExpression("proctime", "PROCTIME()")
                                .build()));
        TableResult tableResult =
                tEnv.executeSql(
                        "SELECT T.aid, T.bid, T.cid\n"
                                + "FROM MyTable\n"
                                + "MATCH_RECOGNIZE (\n"
                                + "  ORDER BY proctime\n"
                                + "  MEASURES\n"
                                + "    `A\"`.id AS aid,\n"
                                + "    \u006C.id AS bid,\n"
                                + "    C.id AS cid\n"
                                + "  PATTERN (`A\"` \u006C C)\n"
                                + "  DEFINE\n"
                                + "    `A\"` AS name = 'a',\n"
                                + "    \u006C AS name = 'b',\n"
                                + "    C AS name = 'c'\n"
                                + ") AS T");
        assertEquals(
                Collections.singletonList(Row.of(6, 7, 8)),
                CollectionUtil.iteratorToList(tableResult.collect()));
    }

    @Test
    public void testSimplePatternWithNulls() {
        tEnv.createTemporaryView(
                "MyTable",
                tEnv.fromDataStream(
                        env.fromElements(
                                        Row.of(1, "a", null),
                                        Row.of(2, "b", null),
                                        Row.of(3, "c", null),
                                        Row.of(4, "d", null),
                                        Row.of(5, null, null),
                                        Row.of(6, "a", null),
                                        Row.of(7, "b", null),
                                        Row.of(8, "c", null),
                                        Row.of(9, null, null))
                                .returns(
                                        ROW_NAMED(
                                                new String[] {"id", "name", "nullField"},
                                                INT,
                                                STRING,
                                                STRING)),
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .column("nullField", DataTypes.STRING())
                                .columnByExpression("proctime", "PROCTIME()")
                                .build()));
        TableResult tableResult =
                tEnv.executeSql(
                        "SELECT T.aid, T.bNull, T.cid, T.aNull\n"
                                + "FROM MyTable\n"
                                + "MATCH_RECOGNIZE (\n"
                                + "  ORDER BY proctime\n"
                                + "  MEASURES\n"
                                + "    A.id AS aid,\n"
                                + "    A.nullField AS aNull,\n"
                                + "    LAST(B.nullField) AS bNull,\n"
                                + "    C.id AS cid\n"
                                + "  PATTERN (A B C)\n"
                                + "  DEFINE\n"
                                + "    A AS name = 'a' AND nullField IS NULL,\n"
                                + "    B AS name = 'b' AND LAST(A.nullField) IS NULL,\n"
                                + "    C AS name = 'c'\n"
                                + ") AS T");
        assertEquals(
                Arrays.asList(Row.of(1, null, 3, null), Row.of(6, null, 8, null)),
                CollectionUtil.iteratorToList(tableResult.collect()));
    }

    @Test
    public void testCodeSplitsAreProperlyGenerated() {
        tEnv.getConfig().setMaxGeneratedCodeLength(1);
        tEnv.createTemporaryView(
                "MyTable",
                tEnv.fromDataStream(
                        env.fromElements(
                                        Row.of(1, "a", "key1", "second_key3"),
                                        Row.of(2, "b", "key1", "second_key3"),
                                        Row.of(3, "c", "key1", "second_key3"),
                                        Row.of(4, "d", "key", "second_key"),
                                        Row.of(5, "e", "key", "second_key"),
                                        Row.of(6, "a", "key2", "second_key4"),
                                        Row.of(7, "b", "key2", "second_key4"),
                                        Row.of(8, "c", "key2", "second_key4"),
                                        Row.of(9, "f", "key", "second_key"))
                                .returns(
                                        ROW_NAMED(
                                                new String[] {"id", "name", "key1", "key2"},
                                                INT,
                                                STRING,
                                                STRING,
                                                STRING)),
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .column("key1", DataTypes.STRING())
                                .column("key2", DataTypes.STRING())
                                .columnByExpression("proctime", "PROCTIME()")
                                .build()));
        TableResult tableResult =
                tEnv.executeSql(
                        "SELECT *\n"
                                + "FROM MyTable\n"
                                + "MATCH_RECOGNIZE (\n"
                                + "  PARTITION BY key1, key2\n"
                                + "  ORDER BY proctime\n"
                                + "  MEASURES\n"
                                + "    A.id AS aid,\n"
                                + "    A.key1 AS akey1,\n"
                                + "    LAST(B.id) AS bid,\n"
                                + "    C.id AS cid,\n"
                                + "    C.key2 AS ckey2\n"
                                + "  PATTERN (A B C)\n"
                                + "  DEFINE\n"
                                + "    A AS name = 'a' AND key1 LIKE '%key%' AND id > 0,\n"
                                + "    B AS name = 'b' AND LAST(A.name, 2) IS NULL,\n"
                                + "    C AS name = 'c' AND LAST(A.name) = 'a'\n"
                                + ") AS T");
        List<Row> actual = CollectionUtil.iteratorToList(tableResult.collect());
        actual.sort(Comparator.comparing(o -> String.valueOf(o.getField(0))));
        assertEquals(
                Arrays.asList(
                        Row.of("key1", "second_key3", 1, "key1", 2, 3, "second_key3"),
                        Row.of("key2", "second_key4", 6, "key2", 7, 8, "second_key4")),
                actual);
    }

    @Test
    public void testLogicalOffsets() {
        tEnv.createTemporaryView(
                "Ticker",
                tEnv.fromDataStream(
                        env.fromElements(
                                        Row.of("ACME", 1L, 19, 1),
                                        Row.of("ACME", 2L, 17, 2),
                                        Row.of("ACME", 3L, 13, 3),
                                        Row.of("ACME", 4L, 20, 4),
                                        Row.of("ACME", 5L, 20, 5),
                                        Row.of("ACME", 6L, 26, 6),
                                        Row.of("ACME", 7L, 20, 7),
                                        Row.of("ACME", 8L, 25, 8))
                                .returns(
                                        ROW_NAMED(
                                                new String[] {"symbol", "tstamp", "price", "tax"},
                                                STRING,
                                                LONG,
                                                INT,
                                                INT)),
                        Schema.newBuilder()
                                .column("symbol", DataTypes.STRING())
                                .column("tstamp", DataTypes.BIGINT())
                                .column("price", DataTypes.INT())
                                .column("tax", DataTypes.INT())
                                .columnByExpression("proctime", "PROCTIME()")
                                .build()));
        TableResult tableResult =
                tEnv.executeSql(
                        "SELECT *\n"
                                + "FROM Ticker\n"
                                + "MATCH_RECOGNIZE (\n"
                                + "  ORDER BY proctime\n"
                                + "  MEASURES\n"
                                + "    FIRST(DOWN.tstamp) AS start_tstamp,\n"
                                + "    LAST(DOWN.tstamp) AS bottom_tstamp,\n"
                                + "    UP.tstamp AS end_tstamp,\n"
                                + "    FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,\n"
                                + "    UP.price + UP.tax AS end_total\n"
                                + "  ONE ROW PER MATCH\n"
                                + "  AFTER MATCH SKIP PAST LAST ROW\n"
                                + "  PATTERN (DOWN{2,} UP)\n"
                                + "  DEFINE\n"
                                + "    DOWN AS price < LAST(DOWN.price, 1) OR LAST(DOWN.price, 1) IS NULL,\n"
                                + "    UP AS price < FIRST(DOWN.price)\n"
                                + ") AS T");
        assertEquals(
                Collections.singletonList(Row.of(6L, 7L, 8L, 33, 33)),
                CollectionUtil.iteratorToList(tableResult.collect()));
    }

    @Test
    public void testLogicalOffsetsWithStarVariable() {
        tEnv.createTemporaryView(
                "Ticker",
                tEnv.fromDataStream(
                        env.fromElements(
                                        Row.of(1, "ACME", 1L, 20),
                                        Row.of(2, "ACME", 2L, 19),
                                        Row.of(3, "ACME", 3L, 18),
                                        Row.of(4, "ACME", 4L, 17),
                                        Row.of(5, "ACME", 5L, 16),
                                        Row.of(6, "ACME", 6L, 15),
                                        Row.of(7, "ACME", 7L, 14),
                                        Row.of(8, "ACME", 8L, 20))
                                .returns(
                                        ROW_NAMED(
                                                new String[] {"id", "symbol", "tstamp", "price"},
                                                INT,
                                                STRING,
                                                LONG,
                                                INT)),
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("symbol", DataTypes.STRING())
                                .column("tstamp", DataTypes.BIGINT())
                                .column("price", DataTypes.INT())
                                .columnByExpression("proctime", "PROCTIME()")
                                .build()));
        TableResult tableResult =
                tEnv.executeSql(
                        "SELECT *\n"
                                + "FROM Ticker\n"
                                + "MATCH_RECOGNIZE (\n"
                                + "  ORDER BY proctime\n"
                                + "  MEASURES\n"
                                + "    FIRST(id, 0) as id0,\n"
                                + "    FIRST(id, 1) as id1,\n"
                                + "    FIRST(id, 2) as id2,\n"
                                + "    FIRST(id, 3) as id3,\n"
                                + "    FIRST(id, 4) as id4,\n"
                                + "    FIRST(id, 5) as id5,\n"
                                + "    FIRST(id, 6) as id6,\n"
                                + "    FIRST(id, 7) as id7,\n"
                                + "    LAST(id, 0) as id8,\n"
                                + "    LAST(id, 1) as id9,\n"
                                + "    LAST(id, 2) as id10,\n"
                                + "    LAST(id, 3) as id11,\n"
                                + "    LAST(id, 4) as id12,\n"
                                + "    LAST(id, 5) as id13,\n"
                                + "    LAST(id, 6) as id14,\n"
                                + "    LAST(id, 7) as id15\n"
                                + "  ONE ROW PER MATCH\n"
                                + "  AFTER MATCH SKIP PAST LAST ROW\n"
                                + "  PATTERN (`DOWN\"`{2,} UP)\n"
                                + "  DEFINE\n"
                                + "    `DOWN\"` AS price < LAST(price, 1) OR LAST(price, 1) IS NULL,\n"
                                + "    UP AS price = FIRST(price) AND price > FIRST(price, 3) AND price = LAST(price, 7)\n"
                                + ") AS T");
        assertEquals(
                Collections.singletonList(Row.of(1, 2, 3, 4, 5, 6, 7, 8, 8, 7, 6, 5, 4, 3, 2, 1)),
                CollectionUtil.iteratorToList(tableResult.collect()));
    }

    @Test
    public void testLogicalOffsetOutsideOfRangeInMeasures() {
        tEnv.createTemporaryView(
                "Ticker",
                tEnv.fromDataStream(
                        env.fromElements(
                                        Row.of("ACME", 1L, 19, 1),
                                        Row.of("ACME", 2L, 17, 2),
                                        Row.of("ACME", 3L, 13, 3),
                                        Row.of("ACME", 4L, 20, 4))
                                .returns(
                                        ROW_NAMED(
                                                new String[] {"symbol", "tstamp", "price", "tax"},
                                                STRING,
                                                LONG,
                                                INT,
                                                INT)),
                        Schema.newBuilder()
                                .column("symbol", DataTypes.STRING())
                                .column("tstamp", DataTypes.BIGINT())
                                .column("price", DataTypes.INT())
                                .column("tax", DataTypes.INT())
                                .columnByExpression("proctime", "PROCTIME()")
                                .build()));
        TableResult tableResult =
                tEnv.executeSql(
                        "SELECT *\n"
                                + "FROM Ticker\n"
                                + "MATCH_RECOGNIZE (\n"
                                + "  ORDER BY proctime\n"
                                + "  MEASURES\n"
                                + "    FIRST(DOWN.price) as first,\n"
                                + "    LAST(DOWN.price) as last,\n"
                                + "    FIRST(DOWN.price, 5) as nullPrice\n"
                                + "  ONE ROW PER MATCH\n"
                                + "  AFTER MATCH SKIP PAST LAST ROW\n"
                                + "  PATTERN (DOWN{2,} UP)\n"
                                + "  DEFINE\n"
                                + "    DOWN AS price < LAST(DOWN.price, 1) OR LAST(DOWN.price, 1) IS NULL,\n"
                                + "    UP AS price > LAST(DOWN.price)\n"
                                + ") AS T");
        assertEquals(
                Collections.singletonList(Row.of(19, 13, null)),
                CollectionUtil.iteratorToList(tableResult.collect()));
    }

    /**
     * This query checks:
     *
     * <p>1. count(D.price) produces 0, because no rows matched to D 2. sum(D.price) produces null,
     * because no rows matched to D 3. aggregates that take multiple parameters work 4. aggregates
     * with expressions work
     */
    @Test
    public void testAggregates() {
        tEnv.getConfig().setMaxGeneratedCodeLength(1);
        tEnv.createTemporaryView(
                "MyTable",
                tEnv.fromDataStream(
                        env.fromElements(
                                        Row.of(1, "a", 1, 0.8, 1),
                                        Row.of(2, "z", 2, 0.8, 3),
                                        Row.of(3, "b", 1, 0.8, 2),
                                        Row.of(4, "c", 1, 0.8, 5),
                                        Row.of(5, "d", 4, 0.1, 5),
                                        Row.of(6, "a", 2, 1.5, 2),
                                        Row.of(7, "b", 2, 0.8, 3),
                                        Row.of(8, "c", 1, 0.8, 2),
                                        Row.of(9, "h", 4, 0.8, 3),
                                        Row.of(10, "h", 4, 0.8, 3),
                                        Row.of(11, "h", 2, 0.8, 3),
                                        Row.of(12, "h", 2, 0.8, 3))
                                .returns(
                                        ROW_NAMED(
                                                new String[] {
                                                    "id", "name", "price", "rate", "weight"
                                                },
                                                INT,
                                                STRING,
                                                INT,
                                                DOUBLE,
                                                INT)),
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .column("price", DataTypes.INT())
                                .column("rate", DataTypes.DOUBLE())
                                .column("weight", DataTypes.INT())
                                .columnByExpression("proctime", "PROCTIME()")
                                .build()));
        tEnv.createTemporarySystemFunction("weightedAvg", new WeightedAvg());
        TableResult tableResult =
                tEnv.executeSql(
                        "SELECT *\n"
                                + "FROM MyTable\n"
                                + "MATCH_RECOGNIZE (\n"
                                + "  ORDER BY proctime\n"
                                + "  MEASURES\n"
                                + "    FIRST(id) as startId,\n"
                                + "    SUM(A.price) AS sumA,\n"
                                + "    COUNT(D.price) AS countD,\n"
                                + "    SUM(D.price) as sumD,\n"
                                + "    weightedAvg(price, weight) as wAvg,\n"
                                + "    AVG(B.price) AS avgB,\n"
                                + "    SUM(B.price * B.rate) as sumExprB,\n"
                                + "    LAST(id) as endId\n"
                                + "  AFTER MATCH SKIP PAST LAST ROW\n"
                                + "  PATTERN (A+ B+ C D? E)\n"
                                + "  DEFINE\n"
                                + "    A AS SUM(A.price) < 6,\n"
                                + "    B AS SUM(B.price * B.rate) < SUM(A.price) AND\n"
                                + "      SUM(B.price * B.rate) > 0.2 AND\n"
                                + "      SUM(B.price) >= 1 AND\n"
                                + "      AVG(B.price) >= 1 AND\n"
                                + "      weightedAvg(price, weight) > 1\n"
                                + ") AS T");
        assertEquals(
                Arrays.asList(
                        Row.of(1, 5, 0L, null, 2L, 3, 3.4D, 8),
                        Row.of(9, 4, 0L, null, 3L, 4, 3.2D, 12)),
                CollectionUtil.iteratorToList(tableResult.collect()));
    }

    @Test
    public void testAggregatesWithNullInputs() {
        tEnv.getConfig().setMaxGeneratedCodeLength(1);
        tEnv.createTemporaryView(
                "MyTable",
                tEnv.fromDataStream(
                        env.fromElements(
                                        Row.of(1, "a", 10),
                                        Row.of(2, "z", 10),
                                        Row.of(3, "b", null),
                                        Row.of(4, "c", null),
                                        Row.of(5, "d", 3),
                                        Row.of(6, "c", 3),
                                        Row.of(7, "c", 3),
                                        Row.of(8, "c", 3),
                                        Row.of(9, "c", 2))
                                .returns(
                                        ROW_NAMED(
                                                new String[] {"id", "name", "price"},
                                                INT,
                                                STRING,
                                                INT)),
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .column("price", DataTypes.INT())
                                .columnByExpression("proctime", "PROCTIME()")
                                .build()));
        tEnv.createTemporarySystemFunction("weightedAvg", new WeightedAvg());
        TableResult tableResult =
                tEnv.executeSql(
                        "SELECT *\n"
                                + "FROM MyTable\n"
                                + "MATCH_RECOGNIZE (\n"
                                + "  ORDER BY proctime\n"
                                + "  MEASURES\n"
                                + "    SUM(A.price) as sumA,\n"
                                + "    COUNT(A.id) as countAId,\n"
                                + "    COUNT(A.price) as countAPrice,\n"
                                + "    COUNT(*) as countAll,\n"
                                + "    COUNT(price) as countAllPrice,\n"
                                + "    LAST(id) as endId\n"
                                + "  AFTER MATCH SKIP PAST LAST ROW\n"
                                + "  PATTERN (A+ C)\n"
                                + "  DEFINE\n"
                                + "    A AS SUM(A.price) < 30,\n"
                                + "    C AS C.name = 'c'\n"
                                + ") AS T");
        assertEquals(
                Collections.singletonList(Row.of(29, 7L, 5L, 8L, 6L, 8)),
                CollectionUtil.iteratorToList(tableResult.collect()));
    }

    @Test
    public void testAccessingCurrentTime() {
        tEnv.createTemporaryView(
                "MyTable",
                tEnv.fromDataStream(
                        env.fromElements(Row.of(1, "a"))
                                .returns(ROW_NAMED(new String[] {"id", "name"}, INT, STRING)),
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .columnByExpression("proctime", "PROCTIME()")
                                .build()));
        TableResult tableResult =
                tEnv.executeSql(
                        "SELECT T.aid\n"
                                + "FROM MyTable\n"
                                + "MATCH_RECOGNIZE (\n"
                                + "  ORDER BY proctime\n"
                                + "  MEASURES\n"
                                + "    A.id AS aid,\n"
                                + "    A.proctime AS aProctime,\n"
                                + "    LAST(A.proctime + INTERVAL '1' second) as calculatedField\n"
                                + "  PATTERN (A)\n"
                                + "  DEFINE\n"
                                + "    A AS proctime >= (CURRENT_TIMESTAMP - INTERVAL '1' day)\n"
                                + ") AS T");
        assertEquals(
                Collections.singletonList(Row.of(1)),
                CollectionUtil.iteratorToList(tableResult.collect()));
    }

    @Test
    public void testUserDefinedFunctions() {
        tEnv.getConfig().setMaxGeneratedCodeLength(1);
        tEnv.createTemporaryView(
                "MyTable",
                tEnv.fromDataStream(
                        env.fromElements(
                                        Row.of(1, "a", 1),
                                        Row.of(2, "a", 1),
                                        Row.of(3, "a", 1),
                                        Row.of(4, "a", 1),
                                        Row.of(5, "a", 1),
                                        Row.of(6, "b", 1),
                                        Row.of(7, "a", 1),
                                        Row.of(8, "a", 1),
                                        Row.of(9, "f", 1))
                                .returns(
                                        ROW_NAMED(
                                                new String[] {"id", "name", "price"},
                                                INT,
                                                STRING,
                                                INT)),
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .column("price", DataTypes.INT())
                                .columnByExpression("proctime", "PROCTIME()")
                                .build()));
        tEnv.createTemporarySystemFunction("prefix", new PrefixingScalarFunc());
        tEnv.createTemporarySystemFunction("countFrom", new RichAggFunc());
        String prefix = "PREF";
        int startFrom = 4;
        Configuration jobParameters = new Configuration();
        jobParameters.setString("prefix", prefix);
        jobParameters.setString("start", Integer.toString(startFrom));
        env.getConfig().setGlobalJobParameters(jobParameters);
        TableResult tableResult =
                tEnv.executeSql(
                        String.format(
                                "SELECT *\n"
                                        + "FROM MyTable\n"
                                        + "MATCH_RECOGNIZE (\n"
                                        + "  ORDER BY proctime\n"
                                        + "  MEASURES\n"
                                        + "    FIRST(id) as firstId,\n"
                                        + "    prefix(A.name) as prefixedNameA,\n"
                                        + "    countFrom(A.price) as countFromA,\n"
                                        + "    LAST(id) as lastId\n"
                                        + "  AFTER MATCH SKIP PAST LAST ROW\n"
                                        + "  PATTERN (A+ C)\n"
                                        + "  DEFINE\n"
                                        + "    A AS prefix(A.name) = '%s:a' AND countFrom(A.price) <= %d\n"
                                        + ") AS T",
                                prefix, 4 + 4));
        assertEquals(
                Arrays.asList(Row.of(1, "PREF:a", 8, 5), Row.of(7, "PREF:a", 6, 9)),
                CollectionUtil.iteratorToList(tableResult.collect()));
    }

    /** Test prefixing function.. */
    public static class PrefixingScalarFunc extends ScalarFunction {

        private String prefix = "ERROR_VALUE";

        @Override
        public void open(FunctionContext context) throws Exception {
            prefix = context.getJobParameter("prefix", "");
        }

        public String eval(String value) {
            return String.format("%s:%s", prefix, value);
        }
    }

    /** Test count accumulator. */
    public static class CountAcc {
        public Integer count;

        public CountAcc(Integer count) {
            this.count = count;
        }
    }

    /** Test rich aggregate function. */
    public static class RichAggFunc extends AggregateFunction<Integer, CountAcc> {

        private Integer start = 0;

        @Override
        public void open(FunctionContext context) throws Exception {
            start = Integer.valueOf(context.getJobParameter("start", "0"));
        }

        @Override
        public void close() throws Exception {
            start = 0;
        }

        @Override
        public CountAcc createAccumulator() {
            return new CountAcc(start);
        }

        @Override
        public Integer getValue(CountAcc accumulator) {
            return accumulator.count;
        }

        public void accumulate(CountAcc countAcc, Integer value) {
            countAcc.count += value;
        }
    }
}
