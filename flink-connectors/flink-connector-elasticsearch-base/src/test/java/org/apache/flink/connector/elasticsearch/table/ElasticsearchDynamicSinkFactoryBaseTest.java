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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.connector.elasticsearch.ElasticsearchUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for validation in {@link ElasticsearchDynamicSinkFactoryBase}. */
@ExtendWith(TestLoggerExtension.class)
abstract class ElasticsearchDynamicSinkFactoryBaseTest {

    abstract ElasticsearchDynamicSinkFactoryBase createSinkFactory();

    abstract TestContext createPrefilledTestContext();

    void assertValidationException(String expectedMessage, Executable executable) {
        ValidationException thrown = Assertions.assertThrows(ValidationException.class, executable);
        Assertions.assertEquals(expectedMessage, thrown.getMessage());
    }

    @Test
    public void validateWrongIndex() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();
        assertValidationException(
                "'index' must not be empty",
                () ->
                        sinkFactory.createDynamicTableSink(
                                createPrefilledTestContext()
                                        .withOption(
                                                ElasticsearchConnectorOptions.INDEX_OPTION.key(),
                                                "")
                                        .build()));
    }

    @Test
    public void validateWrongHosts() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();
        assertValidationException(
                "Could not parse host 'wrong-host' in option 'hosts'. It should follow the format 'http://host_name:port'.",
                () ->
                        sinkFactory.createDynamicTableSink(
                                createPrefilledTestContext()
                                        .withOption(
                                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                                "wrong-host")
                                        .build()));
    }

    @Test
    public void validateWrongFlushSize() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();
        assertValidationException(
                "'sink.bulk-flush.max-size' must be in MB granularity. Got: 1024 bytes",
                () ->
                        sinkFactory.createDynamicTableSink(
                                createPrefilledTestContext()
                                        .withOption(
                                                ElasticsearchConnectorOptions
                                                        .BULK_FLUSH_MAX_SIZE_OPTION
                                                        .key(),
                                                "1kb")
                                        .build()));
    }

    @Test
    public void validateWrongRetries() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();

        assertValidationException(
                "'sink.bulk-flush.backoff.max-retries' must be at least 1. Got: 0",
                () ->
                        sinkFactory.createDynamicTableSink(
                                createPrefilledTestContext()
                                        .withOption(
                                                ElasticsearchConnectorOptions
                                                        .BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION
                                                        .key(),
                                                "0")
                                        .build()));
    }

    @Test
    public void validateWrongMaxActions() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();

        assertValidationException(
                "'sink.bulk-flush.max-actions' must be at least 1. Got: -2",
                () ->
                        sinkFactory.createDynamicTableSink(
                                createPrefilledTestContext()
                                        .withOption(
                                                ElasticsearchConnectorOptions
                                                        .BULK_FLUSH_MAX_ACTIONS_OPTION
                                                        .key(),
                                                "-2")
                                        .build()));
    }

    @Test
    public void validateWrongBackoffDelay() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();

        assertValidationException(
                "Invalid value for option 'sink.bulk-flush.backoff.delay'.",
                () ->
                        sinkFactory.createDynamicTableSink(
                                createPrefilledTestContext()
                                        .withOption(
                                                ElasticsearchConnectorOptions
                                                        .BULK_FLUSH_BACKOFF_DELAY_OPTION
                                                        .key(),
                                                "-1s")
                                        .build()));
    }

    @Test
    public void validatePrimaryKeyOnIllegalColumn() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();

        assertValidationException(
                "The table has a primary key on columns of illegal types: "
                        + "[ARRAY, MAP, MULTISET, ROW, RAW, VARBINARY].",
                () ->
                        sinkFactory.createDynamicTableSink(
                                createPrefilledTestContext()
                                        .withSchema(
                                                new ResolvedSchema(
                                                        Arrays.asList(
                                                                Column.physical(
                                                                        "a",
                                                                        DataTypes.BIGINT()
                                                                                .notNull()),
                                                                Column.physical(
                                                                        "b",
                                                                        DataTypes.ARRAY(
                                                                                        DataTypes
                                                                                                .BIGINT()
                                                                                                .notNull())
                                                                                .notNull()),
                                                                Column.physical(
                                                                        "c",
                                                                        DataTypes.MAP(
                                                                                        DataTypes
                                                                                                .BIGINT(),
                                                                                        DataTypes
                                                                                                .STRING())
                                                                                .notNull()),
                                                                Column.physical(
                                                                        "d",
                                                                        DataTypes.MULTISET(
                                                                                        DataTypes
                                                                                                .BIGINT()
                                                                                                .notNull())
                                                                                .notNull()),
                                                                Column.physical(
                                                                        "e",
                                                                        DataTypes.ROW(
                                                                                        DataTypes
                                                                                                .FIELD(
                                                                                                        "a",
                                                                                                        DataTypes
                                                                                                                .BIGINT()))
                                                                                .notNull()),
                                                                Column.physical(
                                                                        "f",
                                                                        DataTypes.RAW(
                                                                                        Void.class,
                                                                                        VoidSerializer
                                                                                                .INSTANCE)
                                                                                .notNull()),
                                                                Column.physical(
                                                                        "g",
                                                                        DataTypes.BYTES()
                                                                                .notNull())),
                                                        Collections.emptyList(),
                                                        UniqueConstraint.primaryKey(
                                                                "name",
                                                                Arrays.asList(
                                                                        "a", "b", "c", "d", "e",
                                                                        "f", "g"))))
                                        .build()));
    }

    @Test
    public void validateWrongCredential() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();

        assertValidationException(
                "'username' and 'password' must be set at the same time. Got: username 'username' and password ''",
                () ->
                        sinkFactory.createDynamicTableSink(
                                createPrefilledTestContext()
                                        .withOption(
                                                ElasticsearchConnectorOptions.USERNAME_OPTION.key(),
                                                "username")
                                        .withOption(
                                                ElasticsearchConnectorOptions.PASSWORD_OPTION.key(),
                                                "")
                                        .build()));
    }

    @Test
    public void testSinkParallelism() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();
        DynamicTableSink sink =
                sinkFactory.createDynamicTableSink(
                        createPrefilledTestContext()
                                .withOption(SINK_PARALLELISM.key(), "2")
                                .build());
        assertThat(sink).isInstanceOf(ElasticsearchDynamicSink.class);
        ElasticsearchDynamicSink esSink = (ElasticsearchDynamicSink) sink;
        SinkProvider provider =
                (SinkProvider) esSink.getSinkRuntimeProvider(new ElasticsearchUtil.MockContext());
        assertThat(2).isEqualTo(provider.getParallelism().get());
    }
}
