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

package org.apache.flink.state.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Unit tests for the savepoint SQL reader. */
class SavepointDynamicTableSourceTest {

    private static final String STATE_TABLE_DDL =
            "CREATE TABLE state_table (\n"
                    + "  k bigint,\n"
                    + "  KeyedPrimitiveValue bigint,\n"
                    + "  KeyedPojoValue ROW<privateLong bigint, publicLong bigint>,\n"
                    + "  KeyedPrimitiveValueList ARRAY<bigint>,\n"
                    + "  KeyedPrimitiveValueMap MAP<string, bigint>,\n"
                    + "  PRIMARY KEY (k) NOT ENFORCED\n"
                    + ")\n"
                    + "with (\n"
                    + "  'connector' = 'savepoint',\n"
                    + "  'state.path' = 'src/test/resources/table-state',\n"
                    + "  'operator.uid' = 'keyed-state-process-uid'\n"
                    + ")";

    private static final String TYPED_KEY_STATE_PATH = "src/test/resources/table-state-typed-keys";

    @Test
    @SuppressWarnings("unchecked")
    public void testReadKeyedState() throws Exception {
        Configuration config = new Configuration();
        config.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(STATE_TABLE_DDL);
        Table table = tEnv.sqlQuery("SELECT * FROM state_table");
        List<Row> result = tEnv.toDataStream(table).executeAndCollect(100);

        assertThat(result).hasSize(10);

        // Check key
        List<Long> keys =
                result.stream().map(r -> (Long) r.getField("k")).collect(Collectors.toList());
        List<Long> expectedKeys = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        assertThat(keys).containsExactlyInAnyOrderElementsOf(expectedKeys);

        // Check primitive value state
        Set<Long> primitiveValues =
                result.stream()
                        .map(r -> (Long) r.getField("KeyedPrimitiveValue"))
                        .collect(Collectors.toSet());
        assertThat(primitiveValues).containsExactly(1L);

        // Check pojo value state
        Set<Row> pojoValues =
                result.stream()
                        .map(r -> (Row) r.getField("KeyedPojoValue"))
                        .collect(Collectors.toSet());
        assertThat(pojoValues).hasSize(1);
        Row pojoData = pojoValues.iterator().next();
        assertThat(pojoData.getField("publicLong")).isEqualTo(1L);
        assertThat(pojoData.getField("privateLong")).isEqualTo(1L);

        // Check list state
        Set<Tuple2<Long, Long[]>> listValues =
                result.stream()
                        .map(
                                r ->
                                        Tuple2.of(
                                                (Long) r.getField("k"),
                                                (Long[]) r.getField("KeyedPrimitiveValueList")))
                        .flatMap(l -> Set.of(l).stream())
                        .collect(Collectors.toSet());
        assertThat(listValues)
                .hasSize(10)
                .allSatisfy(tuple2 -> assertThat(tuple2.f0).isEqualTo(tuple2.f1[0]));

        // Check map state
        Set<Tuple2<Long, Map<String, Long>>> mapValues =
                result.stream()
                        .map(
                                r ->
                                        Tuple2.of(
                                                (Long) r.getField("k"),
                                                (Map<String, Long>)
                                                        r.getField("KeyedPrimitiveValueMap")))
                        .flatMap(l -> Set.of(l).stream())
                        .collect(Collectors.toSet());
        assertThat(mapValues)
                .hasSize(10)
                .allSatisfy(
                        tuple2 -> {
                            assertThat(tuple2.f1).hasSize(1);
                            String expectedKey = String.valueOf(tuple2.f0);
                            assertThat(tuple2.f1.get(expectedKey)).isEqualTo(tuple2.f0);
                        });
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void testReadKeyedStateWithNullValues() throws Exception {
        Configuration config = new Configuration();
        config.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        final String sql =
                "CREATE TABLE state_table (\n"
                        + "  k bigint,\n"
                        + "  total ROW<privateLong bigint, publicLong bigint>,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ")\n"
                        + "with (\n"
                        + "  'connector' = 'savepoint',\n"
                        + "  'state.path' = 'src/test/resources/table-state-nulls',\n"
                        + "  'operator.uid' = 'keyed-state-process-uid-null'\n"
                        + ")";
        tEnv.executeSql(sql);
        Table table = tEnv.sqlQuery("SELECT * FROM state_table");
        List<Row> result = tEnv.toDataStream(table).executeAndCollect(100);
        assertThat(result).hasSize(5);

        List<Long> keys =
                result.stream().map(row -> (Long) row.getField("k")).collect(Collectors.toList());
        assertThat(keys).containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L);

        // Check pojo value state
        Map<Long, Row> pojoValues =
                result.stream()
                        .collect(
                                Collectors.toMap(
                                        v -> (Long) v.getField("k"),
                                        v -> (Row) v.getField("total")));
        assertThat(pojoValues.get(1L)).isEqualTo(Row.of(1L, 1L));
        assertThat(pojoValues.get(2L)).isEqualTo(Row.of(null, null));
        assertThat(pojoValues.get(3L)).isEqualTo(Row.of(null, null));
        assertThat(pojoValues.get(4L)).isEqualTo(Row.of(4L, 4L));
        assertThat(pojoValues.get(5L)).isEqualTo(Row.of(5L, 5L));
    }

    @Test
    void testReadAvroKeyedState() throws Exception {
        Configuration config = new Configuration();
        config.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        final String sql =
                "CREATE TABLE state_table (\n"
                        + "  k bigint,\n"
                        + "  KeyedSpecificAvroValue ROW<longData bigint>,\n"
                        + "  KeyedGenericAvroValue string,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ")\n"
                        + "with (\n"
                        + "  'connector' = 'savepoint',\n"
                        + "  'state.path' = 'src/test/resources/table-state-avro',\n"
                        + "  'operator.uid' = 'keyed-state-process-uid'\n"
                        + ")";
        tEnv.executeSql(sql);
        Table table = tEnv.sqlQuery("SELECT * FROM state_table");
        List<Row> result = tEnv.toDataStream(table).executeAndCollect(100);
        assertThat(result).hasSize(10);

        // Check key
        List<Long> keys =
                result.stream().map(r -> (Long) r.getField("k")).collect(Collectors.toList());
        List<Long> expectedKeys = LongStream.range(0L, 10L).boxed().collect(Collectors.toList());
        assertThat(keys).containsExactlyInAnyOrderElementsOf(expectedKeys);

        // Check avro value state
        Set<Row> specificAvroValues =
                result.stream()
                        .map(r -> (Row) r.getField("KeyedSpecificAvroValue"))
                        .collect(Collectors.toSet());
        assertThat(specificAvroValues).hasSize(1);
        Row avroData = specificAvroValues.iterator().next();
        assertThat(avroData.getField("longData")).isEqualTo(1L);

        Set<String> genericAvroValues =
                result.stream()
                        .map(r -> (String) r.getField("KeyedGenericAvroValue"))
                        .collect(Collectors.toSet());
        assertThat(genericAvroValues).hasSize(1);
        String avroGenericValue = genericAvroValues.iterator().next();
        assertThat(avroGenericValue).isEqualTo("{\"longData\": 1}");
    }

    // -------------------------------------------------------------------------
    //  Filter push-down tests
    // -------------------------------------------------------------------------

    @Test
    void testFilterPushDownEqualityReturnsOnlyMatchingKey() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE k = 5";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();

        List<Row> result = tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getField("k")).isEqualTo(5L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testFilterPushDownEqualityReturnsCorrectResult() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT * FROM state_table WHERE k = 5";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();

        List<Row> result = tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100);

        assertThat(result).hasSize(1);
        Row row = result.get(0);
        assertThat(row.getField("k")).isEqualTo(5L);
        assertThat(row.getField("KeyedPrimitiveValue")).isEqualTo(1L);

        Row pojo = (Row) row.getField("KeyedPojoValue");
        assertThat(pojo.getField("privateLong")).isEqualTo(1L);
        assertThat(pojo.getField("publicLong")).isEqualTo(1L);

        Long[] list = (Long[]) row.getField("KeyedPrimitiveValueList");
        assertThat(list).containsExactly(5L);

        Map<String, Long> map = (Map<String, Long>) row.getField("KeyedPrimitiveValueMap");
        assertThat(map).containsExactlyEntriesOf(Map.of("5", 5L));
    }

    @Test
    void testFilterPushDownRangeReturnsCorrectResult() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE k >= 7 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();

        List<Row> result = tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100);

        assertThat(result).hasSize(3);
        assertThat(result.get(0).getField("k")).isEqualTo(7L);
        assertThat(result.get(1).getField("k")).isEqualTo(8L);
        assertThat(result.get(2).getField("k")).isEqualTo(9L);
    }

    @Test
    void testFilterPushDownNonexistentKeyReturnsEmpty() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE k = 999";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();

        List<Row> result = tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100);

        assertThat(result).isEmpty();
    }

    @Test
    void testFilterPushDownInListReturnsOnlyMatchingKeys() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE k IN (3, 7) ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();

        List<Row> result = tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100);

        assertThat(result).hasSize(2);
        assertThat(result.get(0).getField("k")).isEqualTo(3L);
        assertThat(result.get(1).getField("k")).isEqualTo(7L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testFilterPushDownPartialPushDown() throws Exception {
        // When the WHERE clause contains both a key filter and a non-key filter,
        // both must be applied correctly regardless of which is pushed into the source.
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql =
                "SELECT k, KeyedPrimitiveValueMap FROM state_table"
                        + " WHERE k = 5 AND KeyedPrimitiveValueMap['5'] > 3";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();

        List<Row> result = tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getField("k")).isEqualTo(5L);
        Map<String, Long> map =
                (Map<String, Long>) result.get(0).getField("KeyedPrimitiveValueMap");
        assertThat(map).containsEntry("5", 5L);
    }

    @Test
    void testFilterPushDownBetweenReturnsCorrectResult() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE k BETWEEN 3 AND 6 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();

        List<Row> result = tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100);

        assertThat(result).hasSize(4);
        assertThat(result.get(0).getField("k")).isEqualTo(3L);
        assertThat(result.get(1).getField("k")).isEqualTo(4L);
        assertThat(result.get(2).getField("k")).isEqualTo(5L);
        assertThat(result.get(3).getField("k")).isEqualTo(6L);
    }

    @Test
    void testFilterPushDownLiteralOnLeftSide() throws Exception {
        // verify that "5 = k" (literal on the left) works the same as "k = 5".
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE 5 = k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();

        List<Row> result = tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getField("k")).isEqualTo(5L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testOrAcrossKeyAndNonKeyColumnIsNotPushedDownButReturnsCorrectResult() throws Exception {
        // OR involving a non-pushable column: correctness must be preserved
        // regardless of whether the planner pushes the filter or not.
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql =
                "SELECT k, KeyedPrimitiveValueMap FROM state_table"
                        + " WHERE k = 5 OR KeyedPrimitiveValueMap['0'] = 0"
                        + " ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isFalse();

        List<Row> result = tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100);

        assertThat(result).hasSize(2);
        assertThat(result.get(0).getField("k")).isEqualTo(0L);
        Map<String, Long> map0 =
                (Map<String, Long>) result.get(0).getField("KeyedPrimitiveValueMap");
        assertThat(map0).containsEntry("0", 0L);
        assertThat(result.get(1).getField("k")).isEqualTo(5L);
    }

    @Test
    void testOrOfExactAndRangeOnKeyIsNotPushedDownButReturnsCorrectResult() throws Exception {
        // The planner hands this over intact as or(equals(k, 1), greaterThan(k, 5)), but OR only
        // merges finite key sets, so a range branch makes the whole disjunction non-pushable.
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE k = 1 OR k > 5 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isFalse();
        assertThat(collectKeys(tEnv, sql)).containsExactly(1L, 6L, 7L, 8L, 9L);
    }

    @Test
    void testOrOfTwoRangesOnKeyIsNotPushedDownButReturnsCorrectResult() throws Exception {
        // Same limitation for "outside a range". This is also the shape the planner produces
        // when it expands a Sarg, which is why a range combined with <> is not pushed either.
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE k < 2 OR k > 7 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isFalse();
        assertThat(collectKeys(tEnv, sql)).containsExactly(0L, 1L, 8L, 9L);
    }

    @Test
    void testUnsupportedFilterIsNotPushedDownButReturnsCorrectResult() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE k % 2 = 0 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isFalse();

        List<Row> result = tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100);

        List<Long> keys =
                result.stream().map(r -> (Long) r.getField("k")).collect(Collectors.toList());
        assertThat(keys).containsExactly(0L, 2L, 4L, 6L, 8L);
    }

    @Test
    void testFilterPushDownUpperBoundReturnsCorrectResult() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE k < 3 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();
        assertThat(collectKeys(tEnv, sql)).containsExactly(0L, 1L, 2L);
    }

    @Test
    void testFilterPushDownStrictLowerBoundReturnsCorrectResult() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE k > 7 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();
        assertThat(collectKeys(tEnv, sql)).containsExactly(8L, 9L);
    }

    @Test
    void testFilterPushDownIntersectingRangesReturnsCorrectResult() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE k >= 3 AND k <= 6 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();
        assertThat(collectKeys(tEnv, sql)).containsExactly(3L, 4L, 5L, 6L);
    }

    @Test
    void testFilterPushDownComparisonWithLiteralOnLeftSide() throws Exception {
        // verify that "5 < k" (literal on the left) works the same as "k > 5".
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        String sql = "SELECT k FROM state_table WHERE 5 < k ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();
        assertThat(collectKeys(tEnv, sql)).containsExactly(6L, 7L, 8L, 9L);
    }

    @Test
    void testFilterPushDownOnIntKey() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(typedKeyDdl("int_key_table", "int", "int-key-state-op"));

        String sql = "SELECT k FROM int_key_table WHERE k = 5";

        assertThat(hasPushedDownFilter(tEnv, sql)).isTrue();
        assertThat(collectTypedKeys(tEnv, sql)).containsExactly(5);

        String rangeSql = "SELECT k FROM int_key_table WHERE k BETWEEN 3 AND 6 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, rangeSql)).isTrue();
        assertThat(collectTypedKeys(tEnv, rangeSql)).containsExactly(3, 4, 5, 6);
    }

    @Test
    void testFilterPushDownOnDoubleKey() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(typedKeyDdl("double_key_table", "double", "double-key-state-op"));

        String equalitySql = "SELECT k FROM double_key_table WHERE k = 5";

        assertThat(hasPushedDownFilter(tEnv, equalitySql)).isTrue();
        assertThat(collectTypedKeys(tEnv, equalitySql)).containsExactly(5.0d);

        // Bounds keep their own type, so the INT literals are converted to the key type here.
        String rangeSql = "SELECT k FROM double_key_table WHERE k BETWEEN 1 AND 3 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, rangeSql)).isTrue();
        assertThat(collectTypedKeys(tEnv, rangeSql)).containsExactly(1.0d, 2.0d, 3.0d);

        // A BIGINT bound beyond the range where doubles are exact still reaches the filter as a
        // BIGINT literal, so the conversion happens on our side.
        String largeBoundSql = "SELECT k FROM double_key_table WHERE k > 9007199254740000";

        assertThat(hasPushedDownFilter(tEnv, largeBoundSql)).isTrue();
        assertThat(collectTypedKeys(tEnv, largeBoundSql)).containsExactly(9007199254740992.0d);

        // 9007199254740993 is 2^53 + 1, which no double holds. The planner folds the literal to
        // the nearest double itself, so the row it asks for is the one keyed 2^53, and the pushed
        // filter agrees with it rather than rounding on its own.
        String beyondExactRangeSql = "SELECT k FROM double_key_table WHERE k = 9007199254740993";

        assertThat(hasPushedDownFilter(tEnv, beyondExactRangeSql)).isTrue();
        assertThat(collectTypedKeys(tEnv, beyondExactRangeSql))
                .containsExactly(9007199254740992.0d);
    }

    @Test
    void testFilterPushDownOnSmallintKey() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(typedKeyDdl("smallint_key_table", "smallint", "smallint-key-state-op"));

        // The key column is bare here, but an INT literal is not converted to a SMALLINT key:
        // only BIGINT and DOUBLE keys take a literal of another numeric type.
        String sql = "SELECT k FROM smallint_key_table WHERE k BETWEEN 3 AND 6 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isFalse();
        assertThat(collectTypedKeys(tEnv, sql))
                .containsExactly((short) 3, (short) 4, (short) 5, (short) 6);

        // The planner rewrites k = 5 into CAST(k AS INT) = 5, leaving no key column to push on.
        // The predicate stays in the query and still returns the right row.
        String equalitySql = "SELECT k FROM smallint_key_table WHERE k = 5";

        assertThat(hasPushedDownFilter(tEnv, equalitySql)).isFalse();
        assertThat(collectTypedKeys(tEnv, equalitySql)).containsExactly((short) 5);
    }

    @Test
    void testFilterPushDownOnTinyintKey() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(typedKeyDdl("tinyint_key_table", "tinyint", "tinyint-key-state-op"));

        // As for SMALLINT, an INT literal is not converted to a TINYINT key.
        String sql = "SELECT k FROM tinyint_key_table WHERE k BETWEEN 3 AND 6 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, sql)).isFalse();
        assertThat(collectTypedKeys(tEnv, sql))
                .containsExactly((byte) 3, (byte) 4, (byte) 5, (byte) 6);

        // As for SMALLINT, k = 5 is rewritten to CAST(k AS INT) = 5 and cannot be pushed.
        String equalitySql = "SELECT k FROM tinyint_key_table WHERE k = 5";

        assertThat(hasPushedDownFilter(tEnv, equalitySql)).isFalse();
        assertThat(collectTypedKeys(tEnv, equalitySql)).containsExactly((byte) 5);
    }

    @Test
    void testFilterPushDownOnDecimalKey() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(typedKeyDdl("decimal_key_table", "decimal(10, 2)", "decimal-key-state-op"));

        // Literal scale equal to the key scale.
        String sameScaleSql = "SELECT k FROM decimal_key_table WHERE k = 5.00";

        assertThat(hasPushedDownFilter(tEnv, sameScaleSql)).isTrue();
        assertThat(collectTypedKeys(tEnv, sameScaleSql)).containsExactly(new BigDecimal("5.00"));

        // Literal scale above the key scale: the planner widens the comparison and casts the key
        // column, so nothing is pushed and the predicate is evaluated on the read rows instead.
        String largerScaleSql = "SELECT k FROM decimal_key_table WHERE k = 5.000";

        assertThat(hasPushedDownFilter(tEnv, largerScaleSql)).isFalse();
        assertThat(collectTypedKeys(tEnv, largerScaleSql)).containsExactly(new BigDecimal("5.00"));

        // A range keeps the key column bare, but an INT bound is not converted to a DECIMAL key,
        // so this is not pushed either.
        String rangeSql = "SELECT k FROM decimal_key_table WHERE k BETWEEN 3 AND 6 ORDER BY k";

        assertThat(hasPushedDownFilter(tEnv, rangeSql)).isFalse();
        assertThat(collectTypedKeys(tEnv, rangeSql))
                .containsExactly(
                        new BigDecimal("3.00"),
                        new BigDecimal("4.00"),
                        new BigDecimal("5.00"),
                        new BigDecimal("6.00"));
    }

    // -------------------------------------------------------------------------
    //  Projection push-down tests
    // -------------------------------------------------------------------------

    @Test
    void testProjectionPushDownSelectKeyAndOneColumn() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        // Projection only: all 10 rows, 2 columns.
        String sql = "SELECT k, KeyedPrimitiveValue FROM state_table ORDER BY k";
        List<Row> result = tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100);

        assertThat(result).hasSize(10);
        for (Row row : result) {
            assertThat(row.getArity()).isEqualTo(2);
            assertThat(row.getField("KeyedPrimitiveValue")).isEqualTo(1L);
        }
        List<Long> keys =
                result.stream().map(r -> (Long) r.getField("k")).collect(Collectors.toList());
        assertThat(keys)
                .containsExactlyElementsOf(
                        LongStream.range(0L, 10L).boxed().collect(Collectors.toList()));

        // Projection combined with filter push-down: applyProjection updates keyColumnIndex to its
        // position in the projected row, but keyFilter holds only the key value so it remains
        // valid regardless of how the key column moves in the output.
        String filteredSql = "SELECT k, KeyedPrimitiveValue FROM state_table WHERE k = 5";
        assertThat(hasPushedDownFilter(tEnv, filteredSql)).isTrue();
        List<Row> filteredResult =
                tEnv.toDataStream(tEnv.sqlQuery(filteredSql)).executeAndCollect(100);
        assertThat(filteredResult).hasSize(1);
        Row row = filteredResult.get(0);
        assertThat(row.getArity()).isEqualTo(2);
        assertThat(row.getField("k")).isEqualTo(5L);
        assertThat(row.getField("KeyedPrimitiveValue")).isEqualTo(1L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testProjectionPushDownAllColumns() throws Exception {
        StreamTableEnvironment tEnv = createBatchTableEnv();
        tEnv.executeSql(STATE_TABLE_DDL);

        List<Row> result =
                tEnv.toDataStream(tEnv.sqlQuery("SELECT * FROM state_table"))
                        .executeAndCollect(100);

        assertThat(result).hasSize(10);
        for (Row row : result) {
            assertThat(row.getArity()).isEqualTo(5);
            assertThat(row.getField("KeyedPrimitiveValue")).isEqualTo(1L);
        }
    }

    // -------------------------------------------------------------------------
    //  Lazy type resolution tests
    // -------------------------------------------------------------------------

    @Test
    void testPlanningSucceedsWithNonexistentSavepointPath() {
        // Planning must never touch the savepoint (metadata I/O or class loading); both are
        // deferred to the scan runtime provider. A nonexistent path/operator would fail
        // immediately if either were resolved eagerly during planning.
        StreamTableEnvironment tEnv = createBatchTableEnv();

        String ddl =
                "CREATE TABLE state_table_missing (\n"
                        + "  k bigint,\n"
                        + "  KeyedPrimitiveValue bigint,\n"
                        + "  PRIMARY KEY (k) NOT ENFORCED\n"
                        + ")\n"
                        + "with (\n"
                        + "  'connector' = 'savepoint',\n"
                        + "  'state.path' = 'src/test/resources/does-not-exist',\n"
                        + "  'operator.uid' = 'nonexistent-operator-uid'\n"
                        + ")";

        tEnv.executeSql(ddl);
        tEnv.executeSql("CREATE TABLE sink (k BIGINT) WITH ('connector' = 'blackhole')");

        assertThatCode(
                        () ->
                                tEnv.compilePlanSql(
                                        "INSERT INTO sink SELECT k FROM state_table_missing"))
                .doesNotThrowAnyException();
    }

    // -------------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------------

    private static List<Object> collectTypedKeys(StreamTableEnvironment tEnv, String sql)
            throws Exception {
        return tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100).stream()
                .map(r -> r.getField("k"))
                .collect(Collectors.toList());
    }

    private static String typedKeyDdl(String table, String keyType, String uid) {
        return "CREATE TABLE "
                + table
                + " (\n"
                + "  k "
                + keyType
                + ",\n"
                + "  v bigint,\n"
                + "  PRIMARY KEY (k) NOT ENFORCED\n"
                + ")\n"
                + "with (\n"
                + "  'connector' = 'savepoint',\n"
                + "  'state.path' = '"
                + TYPED_KEY_STATE_PATH
                + "',\n"
                + "  'operator.uid' = '"
                + uid
                + "'\n"
                + ")";
    }

    private static List<Long> collectKeys(StreamTableEnvironment tEnv, String sql)
            throws Exception {
        return tEnv.toDataStream(tEnv.sqlQuery(sql)).executeAndCollect(100).stream()
                .map(r -> (Long) r.getField("k"))
                .collect(Collectors.toList());
    }

    private static StreamTableEnvironment createBatchTableEnv() {
        Configuration config = new Configuration();
        config.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        return StreamTableEnvironment.create(env);
    }

    private static final Pattern PUSHED_DOWN_FILTER =
            Pattern.compile(
                    "TableSourceScan\\(table=\\[\\[default_catalog, default_database, \\w+,"
                            + " filter=\\[[^\\]]+\\]");

    private static boolean hasPushedDownFilter(StreamTableEnvironment tEnv, String sql) {
        return PUSHED_DOWN_FILTER.matcher(tEnv.explainSql(sql)).find();
    }
}
