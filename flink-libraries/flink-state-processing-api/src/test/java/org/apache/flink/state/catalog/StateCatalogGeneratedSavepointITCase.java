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

package org.apache.flink.state.catalog;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.StateTableUtils;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.state.api.schema.KeyedStateSchemaInfo;
import org.apache.flink.state.table.module.StateModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link StateCatalog} and {@link StateTableUtils} against real keyed-state
 * savepoints that are checked in as test resources, grouped by fixture/scenario in {@code @Nested}
 * classes.
 *
 * <p>These savepoints were produced once with the {@code hashmap} state backend by the (disabled,
 * manually-run) generator programs under {@code src/test/resources/generator} and are checked in
 * under {@code src/test/resources/}. They cannot be regenerated for the RocksDB state backend
 * without running those generators locally against a RocksDB-configured job, so every test in this
 * class is inherently HashMap-only — see {@code KeyedStateReadingITCase} for the
 * RocksDB-parameterized equivalent exercised against savepoints taken at runtime instead of
 * checked-in fixtures.
 */
class StateCatalogGeneratedSavepointITCase {

    /**
     * Schema discovery and reads for a savepoint whose POJO/Avro classes aren't on the classpath.
     */
    @Nested
    class SchemaDiscoveryWithoutSourceClasses {

        private static final String STATE_PATH = "src/test/resources/table-state-missing-class";
        private static final String OPERATOR_UID = "missing-class-operator";
        private static final String AVRO_STATE_PATH = "src/test/resources/table-state-missing-avro";
        private static final String AVRO_OPERATOR_UID = "missing-avro-operator";
        private final String[] avroStateNames = {"KeyedAvroSpecificValue", "KeyedAvroGenericValue"};
        private static final int NUM_KEYS = 10;

        @Test
        @SuppressWarnings("unchecked")
        void testReadKeyedStateFromSchemaDiscovery() throws Exception {
            List<Row> result = readViaTemporaryTable(STATE_PATH, OPERATOR_UID, "state_table");

            assertThat(result).hasSize(NUM_KEYS);
            assertThat(stateKeys(result)).containsExactlyInAnyOrderElementsOf(longRange(NUM_KEYS));

            Set<Long> primitiveValues =
                    result.stream()
                            .map(r -> (Long) r.getField("KeyedPrimitiveValue"))
                            .collect(Collectors.toSet());
            assertThat(primitiveValues).containsExactly(1L);

            Set<Row> pojoValues =
                    result.stream()
                            .map(r -> (Row) r.getField("KeyedPojoValue"))
                            .collect(Collectors.toSet());
            assertThat(pojoValues).hasSize(1);
            Row pojoRow = pojoValues.iterator().next();
            assertThat(pojoRow.getField("privateLong")).isEqualTo(1L);
            assertThat(pojoRow.getField("publicLong")).isEqualTo(1L);

            // Each key holds the single-element list [state_key] and the single map entry
            // {state_key: state_key}.
            for (Row row : result) {
                Long key = (Long) row.getField("state_key");
                assertThat((Long[]) row.getField("KeyedPrimitiveValueList")).containsExactly(key);
                assertThat((Map<Long, Long>) row.getField("KeyedPrimitiveValueMap"))
                        .containsExactly(Map.entry(key, key));
            }
        }

        @Test
        void testFlattenedKeyedStateTables() throws Exception {
            StateCatalog catalog = openCatalogOn(STATE_PATH);
            try {
                String dbName = catalog.listDatabases().get(0);
                String listTable = flatKeyedTable(OPERATOR_UID, "KeyedPrimitiveValueList");
                String mapTable = flatKeyedTable(OPERATOR_UID, "KeyedPrimitiveValueMap");

                // (a) the flattened tables exist and expose a composite primary key
                assertThat(catalog.listTables(dbName)).contains(listTable, mapTable);
                assertThat(catalog.tableExists(new ObjectPath(dbName, listTable))).isTrue();
                assertThat(catalog.tableExists(new ObjectPath(dbName, mapTable))).isTrue();
                assertThat(catalog.tableExists(new ObjectPath(dbName, listTable + "-nonexistent")))
                        .isFalse();

                Schema listSchema = schemaOf(catalog, dbName, listTable);
                assertThat(columnNames(listSchema))
                        .containsExactly("state_key", "list_index", "list_value");
                assertThat(listSchema.getPrimaryKey()).isPresent();
                assertThat(listSchema.getPrimaryKey().get().getColumnNames())
                        .containsExactly("state_key", "list_index");

                Schema mapSchema = schemaOf(catalog, dbName, mapTable);
                assertThat(columnNames(mapSchema))
                        .containsExactly("state_key", "map_key", "map_value");
                assertThat(mapSchema.getPrimaryKey()).isPresent();
                assertThat(mapSchema.getPrimaryKey().get().getColumnNames())
                        .containsExactly("state_key", "map_key");

                // (b) the flattened tables can be read correctly and return the expected data
                TableEnvironment tableEnv = newCatalogTableEnv(catalog, dbName);

                // KeyedPrimitiveValueList holds a single-element list [state_key] per key.
                List<Row> listRows = collectWithSql(tableEnv, "SELECT * FROM `" + listTable + "`");
                assertThat(listRows).hasSize(NUM_KEYS);
                assertThat(stateKeys(listRows))
                        .containsExactlyInAnyOrderElementsOf(longRange(NUM_KEYS));
                for (Row row : listRows) {
                    assertThat(row.getField("list_index")).isEqualTo(0L);
                    assertThat(row.getField("list_value")).isEqualTo(row.getField("state_key"));
                }

                // KeyedPrimitiveValueMap holds a single entry {state_key: state_key} per key.
                List<Row> mapRows = collectWithSql(tableEnv, "SELECT * FROM `" + mapTable + "`");
                assertThat(mapRows).hasSize(NUM_KEYS);
                assertThat(stateKeys(mapRows))
                        .containsExactlyInAnyOrderElementsOf(longRange(NUM_KEYS));
                for (Row row : mapRows) {
                    assertThat(row.getField("map_key")).isEqualTo(row.getField("state_key"));
                    assertThat(row.getField("map_value")).isEqualTo(row.getField("state_key"));
                }

                // state_key filter push-down (SupportsFilterPushDown) prunes to a single key even
                // though state_key is only part of the composite (state_key, list_index/map_key)
                // primary key in the flattened schema.
                for (String table : new String[] {listTable, mapTable}) {
                    List<Row> filtered =
                            collectWithSql(
                                    tableEnv, "SELECT * FROM `" + table + "` WHERE state_key = 3");
                    assertThat(filtered).hasSize(1);
                    assertThat(filtered.get(0).getField("state_key")).isEqualTo(3L);
                }
            } finally {
                catalog.close();
            }
        }

        @Test
        void testSchemaExtractionWithoutPojoClass() throws Exception {
            CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(STATE_PATH);
            KeyedStateSchemaInfo schemaInfo =
                    StateTableUtils.getKeyedStateSchema(
                            metadata, OperatorIdentifier.forUid(OPERATOR_UID));

            assertThat(schemaInfo.keyType.getTypeRoot()).isEqualTo(LogicalTypeRoot.BIGINT);
            assertThat(stateTypeRoot(schemaInfo, "KeyedPrimitiveValue"))
                    .isEqualTo(LogicalTypeRoot.BIGINT);
            assertThat(stateTypeRoot(schemaInfo, "KeyedPrimitiveValueList"))
                    .isEqualTo(LogicalTypeRoot.ARRAY);
            assertThat(stateTypeRoot(schemaInfo, "KeyedPrimitiveValueMap"))
                    .isEqualTo(LogicalTypeRoot.MAP);

            assertThat(stateTypeRoot(schemaInfo, "KeyedPojoValue")).isEqualTo(LogicalTypeRoot.ROW);
            RowType pojoRowType =
                    (RowType) schemaInfo.stateSchemas.get("KeyedPojoValue").logicalType;
            assertThat(pojoRowType.getFieldNames()).contains("privateLong", "publicLong");
            assertThat(fieldTypeRoot(pojoRowType, "privateLong")).isEqualTo(LogicalTypeRoot.BIGINT);
            assertThat(fieldTypeRoot(pojoRowType, "publicLong")).isEqualTo(LogicalTypeRoot.BIGINT);
        }

        @Test
        void testReadAvroKeyedStateFromSchemaDiscovery() throws Exception {
            CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(AVRO_STATE_PATH);
            KeyedStateSchemaInfo schemaInfo =
                    StateTableUtils.getKeyedStateSchema(
                            metadata, OperatorIdentifier.forUid(AVRO_OPERATOR_UID));

            // Both the specific-record and the generic-record state degrade to ROW(longData).
            for (String stateName : avroStateNames) {
                assertThat(stateTypeRoot(schemaInfo, stateName)).isEqualTo(LogicalTypeRoot.ROW);
                RowType rowType = (RowType) schemaInfo.stateSchemas.get(stateName).logicalType;
                assertThat(rowType.getFieldNames()).containsExactly("longData");
            }

            List<Row> result =
                    readViaTemporaryTable(AVRO_STATE_PATH, AVRO_OPERATOR_UID, "avro_state_table");

            assertThat(result).hasSize(NUM_KEYS);
            assertThat(stateKeys(result)).containsExactlyInAnyOrderElementsOf(longRange(NUM_KEYS));
            for (String stateName : avroStateNames) {
                Set<Row> values =
                        result.stream()
                                .map(r -> (Row) r.getField(stateName))
                                .collect(Collectors.toSet());
                assertThat(values).hasSize(1);
                assertThat(values.iterator().next().getField("longData")).isEqualTo(1L);
            }
        }
    }

    /** Schema and data for four operator types (primitive, POJO, Avro-specific, Avro-generic). */
    @Nested
    class MultiOperatorTypeCatalog {

        private static final String RESOURCES_DIR = "src/test/resources/keyed-state-catalog";
        private static final String UID_PRIMITIVE = "primitive-state-op";
        private static final String UID_POJO = "pojo-state-op";
        private static final String UID_AVRO_SPECIFIC = "avro-specific-state-op";
        private static final String UID_AVRO_GENERIC = "avro-generic-state-op";

        private StateCatalog catalog;
        private TableEnvironment tableEnv;
        private String dbName;

        @BeforeEach
        void openCatalog() throws Exception {
            catalog = openCatalogOn(RESOURCES_DIR);
            dbName = catalog.listDatabases().get(0);
            tableEnv = newCatalogTableEnv(catalog, dbName);
        }

        @AfterEach
        void closeCatalog() {
            catalog.close();
        }

        @Test
        void testKeyedStateCatalog() throws Exception {
            List<String> tables = catalog.listTables(dbName);
            assertThat(tables)
                    .contains(
                            keyedTable(UID_PRIMITIVE),
                            keyedTable(UID_POJO),
                            keyedTable(UID_AVRO_SPECIFIC),
                            keyedTable(UID_AVRO_GENERIC));

            // metadata is a view, but listTables includes views too, per the Catalog contract.
            assertThat(catalog.listViews(dbName)).containsExactly(StateCatalog.METADATA_TABLE);
            assertThat(tables).contains(StateCatalog.METADATA_TABLE);

            assertThat(catalog.tableExists(new ObjectPath(dbName, keyedTable(UID_PRIMITIVE))))
                    .isTrue();
            assertThat(catalog.tableExists(new ObjectPath(dbName, StateCatalog.METADATA_TABLE)))
                    .isTrue();
            assertThat(catalog.tableExists(new ObjectPath(dbName, "nonexistent"))).isFalse();

            assertThat(columnNames(schemaOf(catalog, dbName, keyedTable(UID_PRIMITIVE))))
                    .contains("state_key", "count");
            assertThat(columnNames(schemaOf(catalog, dbName, keyedTable(UID_POJO))))
                    .contains("state_key", "profile");
            assertThat(columnNames(schemaOf(catalog, dbName, keyedTable(UID_AVRO_SPECIFIC))))
                    .contains("state_key", "avro_specific");
            assertThat(columnNames(schemaOf(catalog, dbName, keyedTable(UID_AVRO_GENERIC))))
                    .contains("state_key", "avro_generic");

            // Primitive state: 5 distinct int keys
            List<Row> primRows = collectAll(tableEnv, UID_PRIMITIVE);
            assertThat(primRows).hasSize(5);
            assertThat(stateKeys(primRows)).containsExactlyInAnyOrder(1, 2, 3, 4, 5);

            // The remaining operators all hold a nested ROW value under string keys.
            assertNestedRowState(UID_POJO, "profile", "name", "score");
            assertNestedRowState(UID_AVRO_SPECIFIC, "avro_specific", "name", "value");
            assertNestedRowState(UID_AVRO_GENERIC, "avro_generic", "name", "value");
        }

        private void assertNestedRowState(String operatorUid, String column, String... nestedFields)
                throws Exception {
            List<Row> rows = collectAll(tableEnv, operatorUid);
            assertThat(rows).hasSize(5);
            assertThat(stateKeys(rows)).containsExactlyInAnyOrder("1", "2", "3", "4", "5");
            for (Row row : rows) {
                Row nested = (Row) row.getField(column);
                assertThat(nested).isNotNull();
                for (String nestedField : nestedFields) {
                    assertThat(nested.getField(nestedField)).isNotNull();
                }
            }
        }

        @Test
        void testProjectionColumnReorder() throws Exception {
            // Reorder: value column first, key column second
            List<Row> primRows =
                    collectWithSql(
                            tableEnv,
                            "SELECT `count`, state_key FROM `"
                                    + keyedTable(UID_PRIMITIVE)
                                    + "` ORDER BY state_key");

            assertThat(primRows).hasSize(5);
            for (Row row : primRows) {
                assertThat(row.getArity()).isEqualTo(2);
                assertThat(row.getField(0)).isEqualTo(1);
                assertThat(row.getField("count")).isEqualTo(1);
                assertThat(row.getField("state_key")).isIn(1, 2, 3, 4, 5);
            }

            // POJO operator: reorder profile (ROW) before state_key
            List<Row> pojoRows =
                    collectWithSql(
                            tableEnv,
                            "SELECT profile, state_key FROM `"
                                    + keyedTable(UID_POJO)
                                    + "` ORDER BY state_key");

            assertThat(pojoRows).hasSize(5);
            for (Row row : pojoRows) {
                assertThat(row.getArity()).isEqualTo(2);
                Row profile = (Row) row.getField(0);
                assertThat(profile).isNotNull();
                assertThat(profile.getField("name")).isNotNull();
                assertThat(profile.getField("score")).isNotNull();
                assertThat(row.getField("state_key")).isNotNull();
            }
        }

        @Test
        void testProjectionSubsets() throws Exception {
            String primTable = "`" + keyedTable(UID_PRIMITIVE) + "`";

            List<Row> keyOnlyRows =
                    collectWithSql(
                            tableEnv, "SELECT state_key FROM " + primTable + " ORDER BY state_key");
            assertThat(keyOnlyRows).hasSize(5);
            for (Row row : keyOnlyRows) {
                assertThat(row.getArity()).isEqualTo(1);
            }
            assertThat(
                            keyOnlyRows.stream()
                                    .map(r -> r.getField("state_key"))
                                    .collect(Collectors.toList()))
                    .containsExactly(1, 2, 3, 4, 5);

            List<Row> valueOnlyRows = collectWithSql(tableEnv, "SELECT `count` FROM " + primTable);
            assertThat(valueOnlyRows).hasSize(5);
            for (Row row : valueOnlyRows) {
                assertThat(row.getArity()).isEqualTo(1);
                assertThat(row.getField("count")).isEqualTo(1);
            }
        }
    }

    /** POJO-key and Avro-specific-key savepoints — off-classpath key types. */
    @Nested
    class OffClasspathKeyTypes {

        private static final String POJO_AVRO_KEY_DIR =
                "src/test/resources/keyed-state-pojo-avro-key";
        private static final String UID_POJO_KEY = "pojo-key-state-op";
        private static final String UID_AVRO_SPECIFIC_KEY = "avro-specific-key-state-op";

        private StateCatalog catalog;
        private TableEnvironment tableEnv;
        private String dbName;
        private Path savepointPath;

        @BeforeEach
        void openCatalog() throws Exception {
            savepointPath = findSavepointDir(POJO_AVRO_KEY_DIR);
            catalog = openCatalogOn(POJO_AVRO_KEY_DIR);
            dbName = catalog.listDatabases().get(0);
            tableEnv = newCatalogTableEnv(catalog, dbName);
        }

        @AfterEach
        void closeCatalog() {
            catalog.close();
        }

        @Test
        void testPojoAndAvroKeySchemaTypes() throws Exception {
            CheckpointMetadata metadata =
                    SavepointLoader.loadSavepointMetadata(savepointPath.toString());

            // POJO key (PersonKey{int id, String name}) → ROW(id INT, name VARCHAR)
            KeyedStateSchemaInfo pojoSchema =
                    StateTableUtils.getKeyedStateSchema(
                            metadata, OperatorIdentifier.forUid(UID_POJO_KEY));
            assertThat(pojoSchema.keyType.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
            RowType pojoKeyType = (RowType) pojoSchema.keyType;
            assertThat(pojoKeyType.getFieldNames()).containsExactlyInAnyOrder("id", "name");
            assertThat(fieldTypeRoot(pojoKeyType, "id")).isEqualTo(LogicalTypeRoot.INTEGER);
            assertThat(fieldTypeRoot(pojoKeyType, "name")).isEqualTo(LogicalTypeRoot.VARCHAR);

            // Avro specific key (StateTestRecord{String name, long value}) → ROW(name VARCHAR,
            // value BIGINT)
            KeyedStateSchemaInfo avroSchema =
                    StateTableUtils.getKeyedStateSchema(
                            metadata, OperatorIdentifier.forUid(UID_AVRO_SPECIFIC_KEY));
            assertThat(avroSchema.keyType.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
            RowType avroKeyType = (RowType) avroSchema.keyType;
            assertThat(avroKeyType.getFieldNames()).containsExactlyInAnyOrder("name", "value");
        }

        @Test
        void testPojoAndAvroKeyedStateTables() throws Exception {
            assertThat(catalog.listTables(dbName))
                    .contains(keyedTable(UID_POJO_KEY), keyedTable(UID_AVRO_SPECIFIC_KEY));

            for (String uid : new String[] {UID_POJO_KEY, UID_AVRO_SPECIFIC_KEY}) {
                assertThat(columnNames(schemaOf(catalog, dbName, keyedTable(uid))))
                        .contains("state_key", "count");
            }

            // POJO key: 5 rows; PersonKey{id, name} off-classpath → deserialized as ROW
            List<Row> pojoKeyRows = collectAll(tableEnv, UID_POJO_KEY);
            assertThat(pojoKeyRows).hasSize(5);
            for (Row row : pojoKeyRows) {
                Row key = (Row) row.getField("state_key");
                assertThat(key).isNotNull();
                assertThat(key.getField("id")).isIn(1, 2, 3, 4, 5);
                assertThat(key.getField("name")).asString().startsWith("name-");
                assertThat(row.getField("count")).isEqualTo(1);
            }

            // Avro-specific key: 5 rows; StateTestRecord{name, value} off-classpath → ROW via
            // GenericRecord fallback
            List<Row> avroKeyRows = collectAll(tableEnv, UID_AVRO_SPECIFIC_KEY);
            assertThat(avroKeyRows).hasSize(5);
            for (Row row : avroKeyRows) {
                Row key = (Row) row.getField("state_key");
                assertThat(key).isNotNull();
                assertThat(key.getField("name")).asString().startsWith("key-");
                assertThat(key.getField("value")).isIn(1L, 2L, 3L, 4L, 5L);
                assertThat(row.getField("count")).isEqualTo(1);
            }
        }
    }

    /** TupleX key and TupleX value with mixed basic + POJO types. */
    @Nested
    class TupleKeyAndValue {

        private static final String TUPLE_KEY_DIR = "src/test/resources/keyed-state-tuple-key";
        private static final String UID_TUPLE_KEY = "tuple-key-state-op";
        private static final String UID_TUPLE_POJO_VALUE = "tuple-pojo-value-state-op";

        private StateCatalog catalog;
        private TableEnvironment tableEnv;
        private String dbName;
        private Path savepointPath;

        @BeforeEach
        void openCatalog() throws Exception {
            savepointPath = findSavepointDir(TUPLE_KEY_DIR);
            catalog = openCatalogOn(TUPLE_KEY_DIR);
            dbName = catalog.listDatabases().get(0);
            tableEnv = newCatalogTableEnv(catalog, dbName);
        }

        @AfterEach
        void closeCatalog() {
            catalog.close();
        }

        @Test
        void testTupleKeySchemaTypes() throws Exception {
            CheckpointMetadata metadata =
                    SavepointLoader.loadSavepointMetadata(savepointPath.toString());

            // Tuple2<Integer, String> key → ROW(f0 INT NOT NULL, f1 VARCHAR)
            KeyedStateSchemaInfo tupleKeySchema =
                    StateTableUtils.getKeyedStateSchema(
                            metadata, OperatorIdentifier.forUid(UID_TUPLE_KEY));
            assertThat(tupleKeySchema.keyType.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
            RowType tupleKeyType = (RowType) tupleKeySchema.keyType;
            assertThat(tupleKeyType.getFieldNames()).containsExactly("f0", "f1");
            assertThat(fieldTypeRoot(tupleKeyType, "f0")).isEqualTo(LogicalTypeRoot.INTEGER);
            assertThat(fieldTypeRoot(tupleKeyType, "f1")).isEqualTo(LogicalTypeRoot.VARCHAR);

            // Integer key, Tuple2<Long, TuplePojoField> value → value column is ROW(f0 BIGINT, f1
            // ROW(name VARCHAR, score BIGINT))
            KeyedStateSchemaInfo tuplePojoValueSchema =
                    StateTableUtils.getKeyedStateSchema(
                            metadata, OperatorIdentifier.forUid(UID_TUPLE_POJO_VALUE));
            assertThat(tuplePojoValueSchema.keyType.getTypeRoot())
                    .isEqualTo(LogicalTypeRoot.INTEGER);
            assertThat(tuplePojoValueSchema.stateSchemas).containsKey("tuple_pojo");
            LogicalType tupleValueType =
                    tuplePojoValueSchema.stateSchemas.get("tuple_pojo").logicalType;
            assertThat(tupleValueType.getTypeRoot()).isEqualTo(LogicalTypeRoot.ROW);
            RowType tupleValueRowType = (RowType) tupleValueType;
            assertThat(tupleValueRowType.getFieldNames()).containsExactly("f0", "f1");
            assertThat(fieldTypeRoot(tupleValueRowType, "f0")).isEqualTo(LogicalTypeRoot.BIGINT);
            assertThat(fieldTypeRoot(tupleValueRowType, "f1")).isEqualTo(LogicalTypeRoot.ROW);
            RowType innerPojoType =
                    (RowType) tupleValueRowType.getTypeAt(tupleValueRowType.getFieldIndex("f1"));
            assertThat(innerPojoType.getFieldNames()).containsExactlyInAnyOrder("name", "score");
        }

        @Test
        void testTupleKeyedStateTables() throws Exception {
            assertThat(catalog.listTables(dbName))
                    .contains(keyedTable(UID_TUPLE_KEY), keyedTable(UID_TUPLE_POJO_VALUE));

            // Tuple2<Integer, String> key: 5 rows; key fields f0=int, f1=string
            List<Row> tupleKeyRows = collectAll(tableEnv, UID_TUPLE_KEY);
            assertThat(tupleKeyRows).hasSize(5);
            for (Row row : tupleKeyRows) {
                Row key = (Row) row.getField("state_key");
                assertThat(key).isNotNull();
                assertThat(key.getField("f0")).isIn(1, 2, 3, 4, 5);
                assertThat(key.getField("f1")).asString().isEqualTo("k-" + key.getField("f0"));
                assertThat(row.getField("count")).isEqualTo(1);
            }

            // Tuple2<Long, TuplePojoField> value: 5 rows; value row has f0=long, f1=row(name,score)
            List<Row> tuplePojoValueRows = collectAll(tableEnv, UID_TUPLE_POJO_VALUE);
            assertThat(tuplePojoValueRows).hasSize(5);
            for (Row row : tuplePojoValueRows) {
                Integer key = (Integer) row.getField("state_key");
                assertThat(key).isIn(1, 2, 3, 4, 5);
                Row tupleValue = (Row) row.getField("tuple_pojo");
                assertThat(tupleValue).isNotNull();
                assertThat(tupleValue.getField("f0")).isEqualTo((long) key * 10);
                Row pojoField = (Row) tupleValue.getField("f1");
                assertThat(pojoField).isNotNull();
                assertThat(pojoField.getField("name")).isEqualTo("name-" + key);
                assertThat(pojoField.getField("score")).isEqualTo((long) key * 100);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Shared helpers
    // -------------------------------------------------------------------------

    private static StateCatalog openCatalogOn(String resourceDir) throws Exception {
        String catalogRoot = Paths.get(resourceDir).toAbsolutePath().toString();
        StateCatalog catalog =
                new StateCatalog("state", Collections.singletonMap("test", catalogRoot));
        catalog.open();
        return catalog;
    }

    private static TableEnvironment newCatalogTableEnv(StateCatalog catalog, String dbName) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tableEnv.loadModule("state", StateModule.INSTANCE);
        tableEnv.registerCatalog("state", catalog);
        tableEnv.useCatalog("state");
        tableEnv.useDatabase(dbName);
        return tableEnv;
    }

    /** Finds the single {@code savepoint-*} directory nested directly under {@code parentDir}. */
    private static Path findSavepointDir(String parentDir) throws IOException {
        try (var stream = Files.list(Paths.get(parentDir))) {
            return stream.filter(
                            p ->
                                    Files.isDirectory(p)
                                            && p.getFileName().toString().startsWith("savepoint-"))
                    .findFirst()
                    .orElseThrow(() -> new IOException("No savepoint found in " + parentDir));
        }
    }

    private static String keyedTable(String operatorUid) {
        return StateCatalog.OPERATOR_UID_PREFIX + operatorUid + StateCatalog.OPERATOR_TABLE_SUFFIX;
    }

    private static String flatKeyedTable(String operatorUid, String stateName) {
        return StateCatalog.OPERATOR_UID_PREFIX
                + operatorUid
                + "_"
                + stateName
                + StateCatalog.FLAT_STATE_TABLE_SUFFIX;
    }

    private static Schema schemaOf(StateCatalog catalog, String dbName, String tableName)
            throws Exception {
        return ((CatalogTable) catalog.getTable(new ObjectPath(dbName, tableName)))
                .getUnresolvedSchema();
    }

    private static List<String> columnNames(Schema schema) {
        return schema.getColumns().stream()
                .map(Schema.UnresolvedColumn::getName)
                .collect(Collectors.toList());
    }

    private static LogicalTypeRoot stateTypeRoot(
            KeyedStateSchemaInfo schemaInfo, String stateName) {
        KeyedStateSchemaInfo.StateEntryInfo entry = schemaInfo.stateSchemas.get(stateName);
        assertThat(entry).as("state '%s'", stateName).isNotNull();
        return entry.logicalType.getTypeRoot();
    }

    private static LogicalTypeRoot fieldTypeRoot(RowType rowType, String fieldName) {
        return rowType.getTypeAt(rowType.getFieldIndex(fieldName)).getTypeRoot();
    }

    private static Set<Object> stateKeys(List<Row> rows) {
        return rows.stream().map(r -> r.getField("state_key")).collect(Collectors.toSet());
    }

    private static List<Long> longRange(int endExclusive) {
        return LongStream.range(0, endExclusive).boxed().collect(Collectors.toList());
    }

    /**
     * Registers the discovered keyed-state table of {@code operatorUid} as a temporary table and
     * reads it in batch mode via {@code StreamTableEnvironment}, bypassing {@link StateCatalog}.
     */
    private static List<Row> readViaTemporaryTable(
            String statePath, String operatorUid, String tableName) throws Exception {
        Configuration config = new Configuration();
        config.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(config));

        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(statePath);
        OperatorIdentifier opId = OperatorIdentifier.forUid(operatorUid);
        CatalogTable catalogTable =
                StateTableUtils.getStateCatalogTable(
                        metadata,
                        StateTableUtils.getKeyedStateSchema(metadata, opId),
                        statePath,
                        opId);

        CatalogManager catalogManager = ((TableEnvironmentImpl) tEnv).getCatalogManager();
        catalogManager.createTemporaryTable(
                catalogTable,
                catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(tableName)),
                false);

        return tEnv.toDataStream(tEnv.sqlQuery("SELECT * FROM " + tableName))
                .executeAndCollect(100);
    }

    private static List<Row> collectAll(TableEnvironment tEnv, String operatorUid)
            throws Exception {
        return collectWithSql(tEnv, "SELECT * FROM `" + keyedTable(operatorUid) + "`");
    }

    private static List<Row> collectWithSql(TableEnvironment tEnv, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        TableResult result = tEnv.executeSql(sql);
        try (CloseableIterator<Row> it = result.collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }
}
