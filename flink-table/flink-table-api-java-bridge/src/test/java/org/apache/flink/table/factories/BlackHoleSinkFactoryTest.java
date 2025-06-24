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

package org.apache.flink.table.factories;

import org.apache.flink.connector.blackhole.table.BlackHoleTableSinkFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link BlackHoleTableSinkFactory}. */
class BlackHoleSinkFactoryTest {

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("f0", DataTypes.STRING()),
                    Column.physical("f1", DataTypes.BIGINT()),
                    Column.physical("f2", DataTypes.BIGINT()));

    @Test
    void testBlackHole() {
        Map<String, String> properties = new HashMap<>();
        properties.put("connector", "blackhole");

        List<String> partitionKeys = Arrays.asList("f0", "f1");
        DynamicTableSink sink = createTableSink(SCHEMA, partitionKeys, properties);

        assertThat(sink.asSummaryString()).isEqualTo("BlackHole");
        assertThat(sink).isInstanceOf(SupportsPartitioning.class);
    }

    @Test
    void testWrongKey() {
        try {
            Map<String, String> properties = new HashMap<>();
            properties.put("connector", "blackhole");
            properties.put("unknown-key", "1");
            createTableSink(SCHEMA, properties);
        } catch (ValidationException e) {
            Throwable cause = e.getCause();
            assertThat(cause).as(cause.toString()).isInstanceOf(ValidationException.class);
            assertThat(cause.getMessage())
                    .as(cause.getMessage())
                    .contains("Unsupported options:\n\nunknown-key");
            return;
        }
        fail("Should fail by ValidationException.");
    }
}
