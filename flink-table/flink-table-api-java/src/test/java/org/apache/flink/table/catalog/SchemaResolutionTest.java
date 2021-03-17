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

package org.apache.flink.table.catalog;

import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.table.utils.ExpressionResolverMocks;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.api.Expressions.callSql;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isRowtimeAttribute;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isTimeAttribute;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link Schema}, {@link DefaultSchemaResolver}, and {@link ResolvedSchema}. */
public class SchemaResolutionTest {

    private static final String COMPUTED_SQL = "orig_ts - INTERVAL '60' MINUTE";

    private static final ResolvedExpression COMPUTED_COLUMN_RESOLVED =
            new ResolvedExpressionMock(DataTypes.TIMESTAMP(3), () -> COMPUTED_SQL);

    private static final String WATERMARK_SQL = "ts - INTERVAL '5' SECOND";

    private static final ResolvedExpression WATERMARK_RESOLVED =
            new ResolvedExpressionMock(DataTypes.TIMESTAMP(3), () -> WATERMARK_SQL);

    private static final String PROCTIME_SQL = "PROCTIME()";

    private static final ResolvedExpression PROCTIME_RESOLVED =
            new ResolvedExpressionMock(
                    fromLogicalToDataType(new TimestampType(false, TimestampKind.PROCTIME, 3)),
                    () -> PROCTIME_SQL);

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .primaryKeyNamed("primary_constraint", "id") // out of order
                    .column("id", DataTypes.INT().notNull())
                    .column("counter", DataTypes.INT().notNull())
                    .column("payload", "ROW<name STRING, age INT, flag BOOLEAN>")
                    .columnByMetadata("topic", DataTypes.STRING(), true)
                    .columnByExpression("ts", callSql(COMPUTED_SQL)) // out of order API expression
                    .columnByMetadata("orig_ts", DataTypes.TIMESTAMP(3), "timestamp")
                    .watermark("ts", WATERMARK_SQL)
                    .columnByExpression("proctime", PROCTIME_SQL)
                    .build();

    @Test
    public void testSchemaResolution() {
        final ResolvedSchema expectedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", DataTypes.INT().notNull()),
                                Column.physical("counter", DataTypes.INT().notNull()),
                                Column.physical(
                                        "payload",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("age", DataTypes.INT()),
                                                DataTypes.FIELD("flag", DataTypes.BOOLEAN()))),
                                Column.metadata("topic", DataTypes.STRING(), null, true),
                                Column.computed("ts", COMPUTED_COLUMN_RESOLVED),
                                Column.metadata(
                                        "orig_ts", DataTypes.TIMESTAMP(3), "timestamp", false),
                                Column.computed("proctime", PROCTIME_RESOLVED)),
                        Collections.singletonList(new WatermarkSpec("ts", WATERMARK_RESOLVED)),
                        UniqueConstraint.primaryKey(
                                "primary_constraint", Collections.singletonList("id")));

        final ResolvedSchema actualStreamSchema = resolveSchema(SCHEMA, true, true);
        {
            assertThat(actualStreamSchema, equalTo(expectedSchema));
            assertTrue(isRowtimeAttribute(getType(actualStreamSchema, "ts")));
            assertTrue(isProctimeAttribute(getType(actualStreamSchema, "proctime")));
        }

        final ResolvedSchema actualBatchSchema = resolveSchema(SCHEMA, false, true);
        {
            assertThat(actualBatchSchema, equalTo(expectedSchema));
            assertFalse(isRowtimeAttribute(getType(actualBatchSchema, "ts")));
            assertTrue(isProctimeAttribute(getType(actualBatchSchema, "proctime")));
        }
    }

    @Test
    public void testSchemaResolutionErrors() {

        // columns

        testError(
                Schema.newBuilder().fromSchema(SCHEMA).column("id", DataTypes.STRING()).build(),
                "Schema must not contain duplicate column names.");

        testError(
                SCHEMA,
                "Metadata columns are not supported in a schema at the current location.",
                true,
                false);

        testError(
                Schema.newBuilder().columnByExpression("invalid", callSql("INVALID")).build(),
                "Invalid expression for computed column 'invalid'.");

        // time attributes and watermarks

        testError(
                Schema.newBuilder()
                        .column("ts", DataTypes.BOOLEAN())
                        .watermark("ts", callSql(WATERMARK_SQL))
                        .build(),
                "Invalid data type of time field for watermark definition. The field must be of type TIMESTAMP(3) WITHOUT TIME ZONE.");

        testError(
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .watermark("ts", callSql("INVALID"))
                        .build(),
                "Invalid expression for watermark 'WATERMARK FOR `ts` AS [INVALID]'.");

        testError(
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .watermark("other_ts", callSql(WATERMARK_SQL))
                        .build(),
                "Invalid column name 'other_ts' for rowtime attribute in watermark declaration. Available columns are: [ts]");

        testError(
                Schema.newBuilder().fromSchema(SCHEMA).watermark("orig_ts", WATERMARK_SQL).build(),
                "Multiple watermark definitions are not supported yet.");

        testError(
                Schema.newBuilder()
                        .columnByExpression("ts", PROCTIME_SQL)
                        .watermark("ts", WATERMARK_SQL)
                        .build(),
                "A watermark can not be defined for a processing-time attribute.");

        // primary keys

        testError(
                Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("INVALID").build(),
                "Column 'INVALID' does not exist.");

        testError(
                Schema.newBuilder()
                        .column("nullable_col", DataTypes.INT())
                        .primaryKey("nullable_col")
                        .build(),
                "Column 'nullable_col' is nullable.");

        testError(
                Schema.newBuilder()
                        .column("orig_ts", DataTypes.TIMESTAMP(3))
                        .columnByExpression("ts", COMPUTED_SQL)
                        .primaryKey("ts")
                        .build(),
                "Column 'ts' is not a physical column.");

        testError(
                Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id", "id").build(),
                "Invalid primary key 'PK_id_id'. A primary key must not contain duplicate columns. Found: [id]");
    }

    @Test
    public void testUnresolvedSchemaString() {
        assertThat(
                SCHEMA.toString(),
                equalTo(
                        "(\n"
                                + "  `id` INT NOT NULL,\n"
                                + "  `counter` INT NOT NULL,\n"
                                + "  `payload` [ROW<name STRING, age INT, flag BOOLEAN>],\n"
                                + "  `topic` METADATA VIRTUAL,\n"
                                + "  `ts` AS [orig_ts - INTERVAL '60' MINUTE],\n"
                                + "  `orig_ts` METADATA FROM 'timestamp',\n"
                                + "  `proctime` AS [PROCTIME()],\n"
                                + "  WATERMARK FOR `ts` AS [ts - INTERVAL '5' SECOND],\n"
                                + "  CONSTRAINT `primary_constraint` PRIMARY KEY (`id`) NOT ENFORCED\n"
                                + ")"));
    }

    @Test
    public void testResolvedSchemaString() {
        final ResolvedSchema resolvedSchema = resolveSchema(SCHEMA);
        assertThat(
                resolvedSchema.toString(),
                equalTo(
                        "(\n"
                                + "  `id` INT NOT NULL,\n"
                                + "  `counter` INT NOT NULL,\n"
                                + "  `payload` ROW<`name` STRING, `age` INT, `flag` BOOLEAN>,\n"
                                + "  `topic` STRING METADATA VIRTUAL,\n"
                                + "  `ts` TIMESTAMP(3) *ROWTIME* AS orig_ts - INTERVAL '60' MINUTE,\n"
                                + "  `orig_ts` TIMESTAMP(3) METADATA FROM 'timestamp',\n"
                                + "  `proctime` TIMESTAMP(3) NOT NULL *PROCTIME* AS PROCTIME(),\n"
                                + "  WATERMARK FOR `ts`: TIMESTAMP(3) AS ts - INTERVAL '5' SECOND,\n"
                                + "  CONSTRAINT `primary_constraint` PRIMARY KEY (`id`) NOT ENFORCED\n"
                                + ")"));
    }

    @Test
    public void testGeneratedConstraintName() {
        final Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .primaryKey("b", "a")
                        .build();
        assertThat(
                schema.getPrimaryKey().orElseThrow(IllegalStateException::new).getConstraintName(),
                equalTo("PK_b_a"));
    }

    @Test
    public void testSinkRowDataType() {
        final ResolvedSchema resolvedSchema = resolveSchema(SCHEMA);
        final DataType expectedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.INT().notNull()),
                                DataTypes.FIELD("counter", DataTypes.INT().notNull()),
                                DataTypes.FIELD(
                                        "payload",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("age", DataTypes.INT()),
                                                DataTypes.FIELD("flag", DataTypes.BOOLEAN()))),
                                DataTypes.FIELD("orig_ts", DataTypes.TIMESTAMP(3)))
                        .notNull();
        assertThat(resolvedSchema.toSinkRowDataType(), equalTo(expectedDataType));
    }

    @Test
    public void testPhysicalRowDataType() {
        final ResolvedSchema resolvedSchema1 = resolveSchema(SCHEMA);
        final DataType expectedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.INT().notNull()),
                                DataTypes.FIELD("counter", DataTypes.INT().notNull()),
                                DataTypes.FIELD(
                                        "payload",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("age", DataTypes.INT()),
                                                DataTypes.FIELD("flag", DataTypes.BOOLEAN()))))
                        .notNull();

        final DataType physicalDataType1 = resolvedSchema1.toPhysicalRowDataType();
        assertThat(physicalDataType1, equalTo(expectedDataType));

        final ResolvedSchema resolvedSchema2 =
                resolveSchema(Schema.newBuilder().fromRowDataType(physicalDataType1).build());
        assertThat(resolvedSchema2.toPhysicalRowDataType(), equalTo(physicalDataType1));
    }

    @Test
    public void testSourceRowDataType() {
        final ResolvedSchema resolvedSchema = resolveSchema(SCHEMA);
        final DataType expectedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.INT().notNull()),
                                DataTypes.FIELD("counter", DataTypes.INT().notNull()),
                                DataTypes.FIELD(
                                        "payload",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("age", DataTypes.INT()),
                                                DataTypes.FIELD("flag", DataTypes.BOOLEAN()))),
                                DataTypes.FIELD("topic", DataTypes.STRING()),
                                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)),
                                DataTypes.FIELD("orig_ts", DataTypes.TIMESTAMP(3)),
                                DataTypes.FIELD("proctime", DataTypes.TIMESTAMP(3).notNull()))
                        .notNull();
        final DataType sourceRowDataType = resolvedSchema.toSourceRowDataType();
        assertThat(sourceRowDataType, equalTo(expectedDataType));

        assertFalse(isTimeAttribute(sourceRowDataType.getChildren().get(4).getLogicalType()));
        assertFalse(isTimeAttribute(sourceRowDataType.getChildren().get(6).getLogicalType()));
    }

    // --------------------------------------------------------------------------------------------

    private static void testError(Schema schema, String errorMessage) {
        testError(schema, errorMessage, true, true);
    }

    private static void testError(
            Schema schema, String errorMessage, boolean isStreaming, boolean supportsMetadata) {
        try {
            resolveSchema(schema, isStreaming, supportsMetadata);
            fail("Error message expected: " + errorMessage);
        } catch (Throwable t) {
            assertThat(t, FlinkMatchers.containsMessage(errorMessage));
        }
    }

    private static ResolvedSchema resolveSchema(Schema schema) {
        return resolveSchema(schema, true, true);
    }

    private static ResolvedSchema resolveSchema(
            Schema schema, boolean isStreamingMode, boolean supportsMetadata) {
        final SchemaResolver resolver =
                new DefaultSchemaResolver(
                        isStreamingMode,
                        supportsMetadata,
                        new DataTypeFactoryMock(),
                        ExpressionResolverMocks.forSqlExpression(
                                SchemaResolutionTest::resolveSqlExpression));
        return resolver.resolve(schema);
    }

    private static ResolvedExpression resolveSqlExpression(
            String sqlExpression, TableSchema inputSchema) {
        switch (sqlExpression) {
            case COMPUTED_SQL:
                assertThat(
                        inputSchema.getFieldDataType("orig_ts").orElse(null),
                        equalTo(DataTypes.TIMESTAMP(3)));
                return COMPUTED_COLUMN_RESOLVED;
            case WATERMARK_SQL:
                assertThat(
                        inputSchema.getFieldDataType("ts").orElse(null),
                        equalTo(DataTypes.TIMESTAMP(3)));
                return WATERMARK_RESOLVED;
            case PROCTIME_SQL:
                return PROCTIME_RESOLVED;
            default:
                throw new UnsupportedOperationException("Unknown SQL expression.");
        }
    }

    private static LogicalType getType(ResolvedSchema resolvedSchema, String column) {
        return resolvedSchema
                .getColumn(column)
                .orElseThrow(IllegalStateException::new)
                .getDataType()
                .getLogicalType();
    }
}
