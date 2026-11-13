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

package org.apache.flink.table.api.internal.materializedtable;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.MaterializedTableDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.operations.materializedtable.MaterializedTableMergeContext;
import org.apache.flink.table.utils.TableEnvironmentMock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.sql.Timestamp;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DescriptorMaterializedTableMergeContext}. */
class DescriptorMaterializedTableMergeContextTest {

    private static final String PERSISTED_COLUMN_NOT_PRODUCED =
            "Failed to execute CREATE OR ALTER MATERIALIZED TABLE statement.\n"
                    + "Invalid schema. All persisted (physical and metadata) columns in the schema"
                    + " part need to be present in the query part.\n"
                    + "However, %s `missing` could not be found in the query.";

    private TableEnvironmentMock tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironmentMock.getStreamingInstance();
        tEnv.createTable(
                "source",
                TableDescriptor.forConnector("fake")
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.BIGINT().notNull())
                                        .column("ts", DataTypes.TIMESTAMP(3))
                                        .build())
                        .build());
    }

    @Test
    void declaredSchemaIsMergedOntoTheQuerySchema() {
        final MaterializedTableMergeContext context =
                context(
                        Schema.newBuilder()
                                .columnByExpression("next_id", "`id` + 1")
                                .watermark("ts", "`ts` - INTERVAL '1' SECOND")
                                .primaryKey("id")
                                .build());

        assertThat(context.getMergedSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .columnByExpression("next_id", "`id` + 1")
                                .column("id", DataTypes.BIGINT().notNull())
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .watermark("ts", "`ts` - INTERVAL '1' SECOND")
                                .primaryKey("id")
                                .build());
        assertThat(context.hasSchemaDefinition()).isFalse();
        assertThat(context.hasConstraintDefinition()).isTrue();
    }

    @Test
    void declaredPhysicalColumnOverridesTheQueryColumnWithDefaultConversionClass() {
        final MaterializedTableMergeContext context =
                context(
                        Schema.newBuilder()
                                .column("ts", DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class))
                                .build());

        assertThat(context.getMergedSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().notNull())
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .build());
        assertThat(context.hasSchemaDefinition()).isTrue();
        assertThat(context.hasConstraintDefinition()).isTrue();
    }

    @Test
    void withoutDeclaredSchemaTheQuerySchemaIsUsed() {
        final MaterializedTableMergeContext context = context(null);

        assertThat(context.getMergedSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().notNull())
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .build());
        assertThat(context.hasSchemaDefinition()).isFalse();
        assertThat(context.hasConstraintDefinition()).isFalse();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidDeclaredSchemas")
    void invalidDeclaredSchemaIsRejected(String name, Schema declared, String expectedMessage) {
        assertThatThrownBy(() -> context(declared).getMergedSchema())
                .isInstanceOf(ValidationException.class)
                .hasMessage(expectedMessage);
    }

    private static Stream<Arguments> invalidDeclaredSchemas() {
        return Stream.of(
                Arguments.of(
                        "physical not produced by query",
                        Schema.newBuilder().column("missing", DataTypes.INT()).build(),
                        String.format(PERSISTED_COLUMN_NOT_PRODUCED, "physical column")),
                Arguments.of(
                        "persisted metadata not produced by query",
                        Schema.newBuilder()
                                .columnByMetadata("missing", DataTypes.INT(), "key", false)
                                .build(),
                        String.format(PERSISTED_COLUMN_NOT_PRODUCED, "metadata persisted column")),
                Arguments.of(
                        "query column declared twice",
                        Schema.newBuilder()
                                .columnByMetadata("id", DataTypes.BIGINT().notNull(), "key", false)
                                .column("id", DataTypes.BIGINT().notNull())
                                .build(),
                        "A column named 'id' already exists in the schema."),
                Arguments.of(
                        "index",
                        Schema.newBuilder().index("id").build(),
                        "Indexes are not supported for materialized tables."),
                Arguments.of(
                        "immutable columns",
                        Schema.newBuilder().immutableColumns("id").build(),
                        "Immutable columns are not supported for materialized tables."));
    }

    private DescriptorMaterializedTableMergeContext context(@Nullable Schema declared) {
        return new DescriptorMaterializedTableMergeContext(
                MaterializedTableDescriptor.newBuilder().schema(declared).build(),
                tEnv.from("source").getQueryOperation(),
                "SELECT 1",
                tEnv.getCatalogManager());
    }
}
