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

package org.apache.flink.table.planner.operations;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.NopOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for converting conditional column additions to operations. */
class SqlAlterTableAddColumnConverterTest extends SqlNodeToOperationConversionTestBase {

    private static final ObjectIdentifier TABLE_IDENTIFIER =
            ObjectIdentifier.of("builtin", "default", "test_table");

    @ParameterizedTest
    @ValueSource(
            strings = {
                "h int comment 'new column'",
                "h double not null first",
                "h as b * 2 after b",
                "h int metadata from 'mk1' virtual first",
                "h int primary key not enforced"
            })
    void testAddMissingColumnIfNotExists(String columnDefinition) throws Exception {
        prepareTable(false);
        final AlterTableChangeOperation expected =
                (AlterTableChangeOperation) parse("alter table test_table add " + columnDefinition);

        for (String addition : conditionalAdditions(columnDefinition)) {
            final Operation operation = parse("alter table test_table " + addition);
            assertThat(operation).as(addition).isInstanceOf(AlterTableChangeOperation.class);
            final AlterTableChangeOperation alteration = (AlterTableChangeOperation) operation;
            assertThat(alteration.getTableIdentifier()).isEqualTo(TABLE_IDENTIFIER);
            assertThat(alteration.getNewTable().getUnresolvedSchema())
                    .isEqualTo(expected.getNewTable().getUnresolvedSchema());
            assertThat(alteration.getTableChanges())
                    .containsExactlyElementsOf(expected.getTableChanges());
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "c bigint comment 'replacement' first",
                "c as a + 1 after a",
                "g int metadata from 'replacement' virtual first"
            })
    void testSkipExistingColumnIfNotExists(String columnDefinition) throws Exception {
        final CatalogTable originalTable = prepareTable(true);

        for (String addition : conditionalAdditions(columnDefinition)) {
            assertThat(parse("alter table test_table " + addition))
                    .as(addition)
                    .isInstanceOf(NopOperation.class);
            assertThat(
                            catalogManager
                                    .getTable(TABLE_IDENTIFIER)
                                    .get()
                                    .getTable()
                                    .getUnresolvedSchema())
                    .isEqualTo(originalTable.getUnresolvedSchema());
        }
    }

    @Test
    void testQuotedColumnIdentifier() throws Exception {
        final CatalogTable originalTable = prepareTable(false);

        for (String addition : conditionalAdditions("`a` bigint")) {
            assertThat(parse("alter table test_table " + addition))
                    .as(addition)
                    .isInstanceOf(NopOperation.class);
        }
        for (String addition : conditionalAdditions("`A` int")) {
            final Operation operation = parse("alter table test_table " + addition);
            assertThat(operation).as(addition).isInstanceOf(AlterTableChangeOperation.class);
            assertThat(((AlterTableChangeOperation) operation).getNewTable().getUnresolvedSchema())
                    .isEqualTo(
                            Schema.newBuilder()
                                    .fromSchema(originalTable.getUnresolvedSchema())
                                    .column("A", DataTypes.INT())
                                    .build());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"add a bigint", "add column a bigint", "add (a bigint)"})
    void testDuplicateColumnWithoutGuard(String addition) throws Exception {
        prepareTable(false);

        assertThatThrownBy(() -> parse("alter table test_table " + addition))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Column `a` already exists in the table.");
    }

    @Test
    void testInvalidMissingColumnDefinition() throws Exception {
        prepareTable(false);

        for (String addition : conditionalAdditions("h int after missing")) {
            assertThatThrownBy(() -> parse("alter table test_table " + addition))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            "Referenced column `missing` by 'AFTER' does not exist in the table.");
        }
        for (String addition : conditionalAdditions("h as missing + 1")) {
            assertThatThrownBy(() -> parse("alter table test_table " + addition))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("Invalid expression for computed column 'h'.");
        }
        for (String addition : conditionalAdditions("e.f3 int")) {
            assertThatThrownBy(() -> parse("alter table test_table " + addition))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining(
                            "Altering the nested row type `e`.`f3` is not supported yet.");
        }
    }

    @Test
    void testMissingTable() {
        for (String addition : conditionalAdditions("h int")) {
            assertThat(parse("alter table if exists missing " + addition))
                    .isInstanceOf(NopOperation.class);
            assertThatThrownBy(() -> parse("alter table missing " + addition))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            "Table `builtin`.`default`.`missing` doesn't exist or is a temporary table.");
        }
        assertThat(parse("alter table if exists missing add if not exists (h int, i string)"))
                .isInstanceOf(NopOperation.class);
    }

    @Test
    void testAddMultipleMissingColumns() throws Exception {
        prepareTable(false);
        final String columns =
                "h int first, i as h + b after h, " + "j int metadata from 'mk1' virtual after i";
        final AlterTableChangeOperation expected =
                (AlterTableChangeOperation) parse("alter table test_table add (" + columns + ")");

        final Operation operation =
                parse("alter table test_table add if not exists (" + columns + ")");

        assertThat(operation).isInstanceOf(AlterTableChangeOperation.class);
        final AlterTableChangeOperation alteration = (AlterTableChangeOperation) operation;
        assertThat(alteration.getNewTable().getUnresolvedSchema())
                .isEqualTo(expected.getNewTable().getUnresolvedSchema());
        assertThat(alteration.getTableChanges())
                .containsExactlyElementsOf(expected.getTableChanges());
    }

    @Test
    void testAddOnlyMissingColumns() throws Exception {
        final CatalogTable originalTable = prepareTable(true);
        final AlterTableChangeOperation expected =
                (AlterTableChangeOperation)
                        parse("alter table test_table add (h int after c, i as h + a after h)");

        final Operation operation =
                parse(
                        "alter table test_table add if not exists ("
                                + "c bigint comment 'replacement' after missing, "
                                + "h int after c, "
                                + "g int metadata from 'replacement' virtual first, "
                                + "i as h + a after h)");

        assertThat(operation).isInstanceOf(AlterTableChangeOperation.class);
        final AlterTableChangeOperation alteration = (AlterTableChangeOperation) operation;
        assertThat(alteration.getNewTable().getUnresolvedSchema())
                .isEqualTo(expected.getNewTable().getUnresolvedSchema());
        assertThat(alteration.getTableChanges())
                .containsExactlyElementsOf(expected.getTableChanges());
        assertThat(catalogManager.getTable(TABLE_IDENTIFIER).get().getTable().getUnresolvedSchema())
                .isEqualTo(originalTable.getUnresolvedSchema());
    }

    @Test
    void testSkipAllExistingColumns() throws Exception {
        final CatalogTable originalTable = prepareTable(true);

        assertThat(
                        parse(
                                "alter table test_table add if not exists ("
                                        + "c bigint first, "
                                        + "g int metadata from 'replacement' virtual, "
                                        + "a int primary key not enforced)"))
                .isInstanceOf(NopOperation.class);
        assertThat(catalogManager.getTable(TABLE_IDENTIFIER).get().getTable().getUnresolvedSchema())
                .isEqualTo(originalTable.getUnresolvedSchema());
    }

    @Test
    void testSkipPrimaryKeyOfExistingColumn() throws Exception {
        prepareTable(true);
        final AlterTableChangeOperation expected =
                (AlterTableChangeOperation) parse("alter table test_table add h int");

        final Operation operation =
                parse(
                        "alter table test_table add if not exists "
                                + "(c bigint primary key not enforced, h int)");

        assertThat(operation).isInstanceOf(AlterTableChangeOperation.class);
        final AlterTableChangeOperation alteration = (AlterTableChangeOperation) operation;
        assertThat(alteration.getNewTable().getUnresolvedSchema())
                .isEqualTo(expected.getNewTable().getUnresolvedSchema());
        assertThat(alteration.getTableChanges())
                .containsExactlyElementsOf(expected.getTableChanges());
    }

    @Test
    void testDuplicateMissingColumnInList() throws Exception {
        prepareTable(false);

        assertThatThrownBy(
                        () -> parse("alter table test_table add if not exists (h int, h string)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Encounter duplicate column `h`.");
    }

    private static List<String> conditionalAdditions(String columnDefinition) {
        return List.of(
                "add column if not exists " + columnDefinition,
                "add if not exists " + columnDefinition,
                "add if not exists (" + columnDefinition + ")");
    }

    private CatalogTable prepareTable(boolean withPrimaryKey) {
        final Schema.Builder schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.BIGINT().notNull())
                        .column("c", DataTypes.STRING().notNull())
                        .withComment("original comment")
                        .column("e", DataTypes.ROW(DataTypes.INT()))
                        .columnByMetadata("g", DataTypes.STRING(), null, true)
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .watermark("ts", "ts - INTERVAL '5' SECOND");
        if (withPrimaryKey) {
            schema.primaryKey("a");
        }
        final CatalogTable table =
                CatalogTable.newBuilder()
                        .schema(schema.build())
                        .comment("original table comment")
                        .partitionKeys(List.of("b", "c"))
                        .options(Map.of("connector", "dummy", "k", "v"))
                        .build();
        catalogManager.createTable(table, TABLE_IDENTIFIER, false);
        return table;
    }
}
