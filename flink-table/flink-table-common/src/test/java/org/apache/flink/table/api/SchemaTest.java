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

package org.apache.flink.table.api;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TestSchemaResolver;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaTest {
    private static final String COMPUTED_SQL = "orig_ts - INTERVAL '60' MINUTE";

    private static final ResolvedExpression COMPUTED_COLUMN_RESOLVED =
            new ResolvedExpressionMock(DataTypes.TIMESTAMP(3), () -> COMPUTED_SQL);

    @Nested
    class Builder {
        @Test
        void testFromResolvedSchema() {
            ResolvedSchema originalSchema =
                    ResolvedSchema.of(
                            Column.physical("event_timestamp", DataTypes.TIMESTAMP_LTZ(3).notNull())
                                    .withComment("The timestamp when the event occurred."),
                            Column.computed("rowtime", COMPUTED_COLUMN_RESOLVED)
                                    .withComment("The rowtime derived from the event timestamp."),
                            Column.metadata("topic", DataTypes.STRING(), null, true)
                                    .withComment("kafka topic"));
            Schema newSchema = Schema.newBuilder().fromResolvedSchema(originalSchema).build();

            assertThat(newSchema.resolve(new TestSchemaResolver())).isEqualTo(originalSchema);
        }
    }
}
