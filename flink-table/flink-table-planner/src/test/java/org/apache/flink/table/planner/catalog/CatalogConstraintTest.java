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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.DefaultIndex;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableSet;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for Catalog constraints. */
public class CatalogConstraintTest {

    private String databaseName = "default_database";

    private TableEnvironment tEnv;
    private Catalog catalog;

    @BeforeEach
    void setup() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        tEnv = TableEnvironment.create(settings);
        catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).orElse(null);
        assertThat(catalog).isNotNull();
    }

    @ParameterizedTest()
    @ValueSource(booleans = {true, false})
    void testWithPrimaryKey(boolean containsPrimaryKey) throws Exception {
        ResolvedSchema resolvedSchema = buildResolvedSchema(containsPrimaryKey);
        final Schema tableSchema = Schema.newBuilder().fromResolvedSchema(resolvedSchema).build();
        Map<String, String> properties = buildCatalogTableProperties(true);

        catalog.createTable(
                new ObjectPath(databaseName, "T1"),
                CatalogTable.newBuilder()
                        .schema(tableSchema)
                        .comment("")
                        .options(properties)
                        .build(),
                false);

        RelNode t1 = TableTestUtil.toRelNode(tEnv.sqlQuery("select * from T1"));
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
        if (containsPrimaryKey) {
            assertThat(mq.getUniqueKeys(t1)).isEqualTo(ImmutableSet.of(ImmutableBitSet.of(1)));
        } else {
            assertThat(mq.getUniqueKeys(t1)).isEqualTo(ImmutableSet.of());
        }
    }

    @ParameterizedTest()
    @ValueSource(booleans = {true, false})
    void testWithImmutableColsConstraint(boolean containsImmutableColsConstraint) throws Exception {
        final Schema.Builder schemaBuilder =
                Schema.newBuilder().fromResolvedSchema(buildResolvedSchema(true));
        if (containsImmutableColsConstraint) {
            schemaBuilder.immutableColumnsNamed("immutable_constraint", List.of("a"));
        }
        final Schema tableSchema = schemaBuilder.build();
        Map<String, String> properties = buildCatalogTableProperties(false);

        catalog.createTable(
                new ObjectPath(databaseName, "T1"),
                CatalogTable.newBuilder()
                        .schema(tableSchema)
                        .comment("")
                        .options(properties)
                        .build(),
                false);

        RelNode t1 = TableTestUtil.toRelNode(tEnv.sqlQuery("select * from T1"));

        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
        // unique keys are not changed
        assertThat(mq.getUniqueKeys(t1)).isEqualTo(ImmutableSet.of(ImmutableBitSet.of(1)));

        if (containsImmutableColsConstraint) {
            assertThat((Iterable<? extends Integer>) mq.getImmutableColumns(t1))
                    .isEqualTo(ImmutableBitSet.of(0, 1));
            assertThat(mq.getUpsertKeys(t1))
                    .isEqualTo(ImmutableSet.of(ImmutableBitSet.of(1), ImmutableBitSet.of(0, 1)));
        } else {
            assertThat((Iterable<? extends Integer>) mq.getImmutableColumns(t1))
                    .isEqualTo(ImmutableBitSet.of(1));
            assertThat(mq.getUpsertKeys(t1)).isEqualTo(ImmutableSet.of(ImmutableBitSet.of(1)));
        }
    }

    @Test
    void testImmutableColsConstraintDefinedOnSourceWithDelete() throws Exception {
        final Schema tableSchema =
                Schema.newBuilder()
                        .fromResolvedSchema(buildResolvedSchema(true))
                        .immutableColumnsNamed("immutable_constraint", List.of("a"))
                        .build();

        Map<String, String> properties = buildCatalogTableProperties(false);
        properties.put("changelog-mode", "I,UA,UB,D");

        catalog.createTable(
                new ObjectPath(databaseName, "T1"),
                CatalogTable.newBuilder()
                        .schema(tableSchema)
                        .comment("")
                        .options(properties)
                        .build(),
                false);

        assertThatThrownBy(() -> TableTestUtil.toRelNode(tEnv.sqlQuery("select * from T1")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The immutable constraint cannot be defined "
                                + "on the table with changelog mode [DELETE].");
    }

    private ResolvedSchema buildResolvedSchema(boolean containsPrimaryKey) {
        return containsPrimaryKey
                ? new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.STRING()),
                                Column.physical("b", DataTypes.BIGINT().notNull()),
                                Column.physical("c", DataTypes.INT())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey(
                                "primary_constraint", Collections.singletonList("b")),
                        Collections.singletonList(DefaultIndex.newIndex("idx", List.of("a", "b"))),
                        null)
                : ResolvedSchema.of(
                        Column.physical("a", DataTypes.BIGINT()),
                        Column.physical("b", DataTypes.STRING()),
                        Column.physical("c", DataTypes.INT()));
    }

    private Map<String, String> buildCatalogTableProperties(boolean legacyTable) {
        Map<String, String> properties = new HashMap<>();
        if (legacyTable) {
            properties.put("connector.type", "filesystem");
            properties.put("connector.property-version", "1");
            properties.put("connector.path", "/path/to/csv");

            properties.put("format.type", "csv");
            properties.put("format.property-version", "1");
            properties.put("format.field-delimiter", ";");
        } else {
            properties.put("connector", "values");
        }

        return properties;
    }
}
