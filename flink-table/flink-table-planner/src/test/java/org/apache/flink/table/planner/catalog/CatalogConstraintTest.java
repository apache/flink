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
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for Catalog constraints. */
public class CatalogConstraintTest {

    private String databaseName = "default_database";

    private TableEnvironment tEnv;
    private Catalog catalog;

    @Before
    public void setup() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        tEnv = TableEnvironment.create(settings);
        catalog = tEnv.getCatalog(tEnv.getCurrentCatalog()).orElse(null);
        assertThat(catalog).isNotNull();
    }

    @Test
    public void testWithPrimaryKey() throws Exception {
        final Schema tableSchema =
                Schema.newBuilder()
                        .fromResolvedSchema(
                                new ResolvedSchema(
                                        Arrays.asList(
                                                Column.physical("a", DataTypes.STRING()),
                                                Column.physical("b", DataTypes.BIGINT().notNull()),
                                                Column.physical("c", DataTypes.INT())),
                                        Collections.emptyList(),
                                        UniqueConstraint.primaryKey(
                                                "primary_constraint",
                                                Collections.singletonList("b"))))
                        .build();
        Map<String, String> properties = buildCatalogTableProperties(tableSchema);

        catalog.createTable(
                new ObjectPath(databaseName, "T1"),
                CatalogTable.of(tableSchema, "", Collections.emptyList(), properties),
                false);

        RelNode t1 = TableTestUtil.toRelNode(tEnv.sqlQuery("select * from T1"));
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
        assertThat(mq.getUniqueKeys(t1)).isEqualTo(ImmutableSet.of(ImmutableBitSet.of(1)));
    }

    @Test
    public void testWithoutPrimaryKey() throws Exception {

        final Schema tableSchema =
                Schema.newBuilder()
                        .fromResolvedSchema(
                                ResolvedSchema.of(
                                        Column.physical("a", DataTypes.BIGINT()),
                                        Column.physical("b", DataTypes.STRING()),
                                        Column.physical("c", DataTypes.INT())))
                        .build();
        Map<String, String> properties = buildCatalogTableProperties(tableSchema);

        catalog.createTable(
                new ObjectPath(databaseName, "T1"),
                CatalogTable.of(tableSchema, "", Collections.emptyList(), properties),
                false);

        RelNode t1 = TableTestUtil.toRelNode(tEnv.sqlQuery("select * from T1"));
        FlinkRelMetadataQuery mq =
                FlinkRelMetadataQuery.reuseOrCreate(t1.getCluster().getMetadataQuery());
        assertThat(mq.getUniqueKeys(t1)).isEqualTo(ImmutableSet.of());
    }

    private Map<String, String> buildCatalogTableProperties(Schema tableSchema) {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector.type", "filesystem");
        properties.put("connector.property-version", "1");
        properties.put("connector.path", "/path/to/csv");

        properties.put("format.type", "csv");
        properties.put("format.property-version", "1");
        properties.put("format.field-delimiter", ";");

        return properties;
    }
}
