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
import org.apache.flink.table.api.MaterializedPipeline;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.table.utils.TableEnvironmentMock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MaterializedPipelineImpl}. */
class MaterializedPipelineImplTest {

    private static final ObjectIdentifier MT =
            ObjectIdentifier.of(
                    CatalogManagerMocks.DEFAULT_CATALOG,
                    CatalogManagerMocks.DEFAULT_DATABASE,
                    "mt");

    private static final TableDescriptor TABLE_DESCRIPTOR =
            TableDescriptor.forConnector("fake")
                    .schema(Schema.newBuilder().column("f0", DataTypes.BIGINT()).build())
                    .build();

    private static final String NOT_EXPRESSIBLE_IN_SQL =
            "The query of a materialized table must be expressible in SQL.";

    private TableEnvironmentMock tEnv;

    @BeforeEach
    void setUp() {
        tEnv = TableEnvironmentMock.getStreamingInstance();
        tEnv.createTable("source", TABLE_DESCRIPTOR);
    }

    @Test
    void regularTableAtThePathIsRejected() throws Exception {
        tEnv.createTable("mt", TABLE_DESCRIPTOR);
        final MaterializedPipeline pipeline = tEnv.from("source").materializeInto("mt");

        assertThatThrownBy(pipeline::execute)
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Cannot create or alter materialized table default_catalog.default_database.mt,"
                                + " because a regular table with the same identifier exists."
                                + " Table#materializeInto does not support regular table conversion.");

        assertThat(getPermanentTableKind()).contains(TableKind.TABLE);
    }

    @Test
    void temporaryTableAtThePathIsRejected() throws Exception {
        tEnv.createTemporaryTable("mt", TABLE_DESCRIPTOR);
        final MaterializedPipeline pipeline = tEnv.from("source").materializeInto("mt");

        assertThatThrownBy(pipeline::execute)
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Cannot create or alter materialized table default_catalog.default_database.mt,"
                                + " because a temporary table or view with the same identifier"
                                + " exists and would shadow it. Drop the temporary object first.");

        assertThat(tEnv.listTemporaryTables()).containsExactly("mt");
        assertThat(getPermanentTableKind()).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidFreshnesses")
    void invalidFreshnessFailsAtMaterializeInto(Duration freshness, String expectedMessage)
            throws Exception {
        final Table query = tEnv.from("source");

        assertThatThrownBy(() -> query.materializeInto("mt", freshness))
                .isInstanceOf(ValidationException.class)
                .hasMessage(expectedMessage);

        assertThat(getPermanentTableKind()).isEmpty();
    }

    private static Stream<Arguments> invalidFreshnesses() {
        return Stream.of(
                Arguments.of(Duration.ZERO, "Freshness must be positive, but was: PT0S"),
                Arguments.of(
                        Duration.ofMillis(1500),
                        "Freshness must be a whole number of seconds, but was: PT1.5S"),
                Arguments.of(
                        Duration.ofSeconds(Integer.MAX_VALUE + 1L),
                        "Freshness must be at most 2147483647 seconds, but was: PT596523H14M8S"));
    }

    @Test
    void inlineFunctionFailsAtMaterializeInto() throws Exception {
        final Table query = tEnv.from("source").select(call(new PlusOne(), $("f0")));

        assertThatThrownBy(() -> query.materializeInto("mt"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(NOT_EXPRESSIBLE_IN_SQL);

        assertThat(getPermanentTableKind()).isEmpty();
    }

    @Test
    void tableFromDescriptorFailsAtMaterializeInto() throws Exception {
        final Table query = tEnv.from(TABLE_DESCRIPTOR);

        assertThatThrownBy(() -> query.materializeInto("mt"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(NOT_EXPRESSIBLE_IN_SQL);

        assertThat(getPermanentTableKind()).isEmpty();
    }

    private Optional<TableKind> getPermanentTableKind() throws Exception {
        final Catalog catalog =
                tEnv.getCatalogManager().getCatalog(MT.getCatalogName()).orElseThrow();
        if (!catalog.tableExists(MT.toObjectPath())) {
            return Optional.empty();
        }
        return Optional.of(catalog.getTable(MT.toObjectPath()).getTableKind());
    }

    /** A user-defined function for the inline function case. */
    public static class PlusOne extends ScalarFunction {
        public Long eval(Long value) {
            return value == null ? null : value + 1;
        }
    }
}
