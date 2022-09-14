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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.RAW;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for validation in {@link ElasticsearchDynamicSinkFactoryBase}. */
@ExtendWith(TestLoggerExtension.class)
abstract class ElasticsearchDynamicSinkFactoryBaseTest {

    abstract ElasticsearchDynamicSinkFactoryBase createSinkFactory();

    abstract TestContext createPrefilledTestContext();

    @Test
    public void validateWrongIndex() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();
        assertThatThrownBy(
                        () ->
                                sinkFactory.createDynamicTableSink(
                                        createPrefilledTestContext()
                                                .withOption(
                                                        ElasticsearchConnectorOptions.INDEX_OPTION
                                                                .key(),
                                                        "")
                                                .build()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("'index' must not be empty");
    }

    @Test
    public void validateWrongHosts() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();
        assertThatThrownBy(
                        () ->
                                sinkFactory.createDynamicTableSink(
                                        createPrefilledTestContext()
                                                .withOption(
                                                        ElasticsearchConnectorOptions.HOSTS_OPTION
                                                                .key(),
                                                        "wrong-host")
                                                .build()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Could not parse host 'wrong-host' in option 'hosts'. It should follow the format 'http://host_name:port'.");
    }

    @Test
    public void validateWrongFlushSize() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();
        assertThatThrownBy(
                        () ->
                                sinkFactory.createDynamicTableSink(
                                        createPrefilledTestContext()
                                                .withOption(
                                                        ElasticsearchConnectorOptions
                                                                .BULK_FLUSH_MAX_SIZE_OPTION
                                                                .key(),
                                                        "1kb")
                                                .build()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "'sink.bulk-flush.max-size' must be in MB granularity. Got: 1024 bytes");
    }

    @Test
    public void validateWrongRetries() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();

        assertThatThrownBy(
                        () ->
                                sinkFactory.createDynamicTableSink(
                                        createPrefilledTestContext()
                                                .withOption(
                                                        ElasticsearchConnectorOptions
                                                                .BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION
                                                                .key(),
                                                        "0")
                                                .build()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("'sink.bulk-flush.backoff.max-retries' must be at least 1. Got: 0");
    }

    @Test
    public void validateWrongMaxActions() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();

        assertThatThrownBy(
                        () ->
                                sinkFactory.createDynamicTableSink(
                                        createPrefilledTestContext()
                                                .withOption(
                                                        ElasticsearchConnectorOptions
                                                                .BULK_FLUSH_MAX_ACTIONS_OPTION
                                                                .key(),
                                                        "-2")
                                                .build()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("'sink.bulk-flush.max-actions' must be at least 1. Got: -2");
    }

    @Test
    public void validateWrongBackoffDelay() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();

        assertThatThrownBy(
                        () ->
                                sinkFactory.createDynamicTableSink(
                                        createPrefilledTestContext()
                                                .withOption(
                                                        ElasticsearchConnectorOptions
                                                                .BULK_FLUSH_BACKOFF_DELAY_OPTION
                                                                .key(),
                                                        "-1s")
                                                .build()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Invalid value for option 'sink.bulk-flush.backoff.delay'.");
    }

    @Test
    public void validatePrimaryKeyOnIllegalColumn() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();

        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", BIGINT().notNull()),
                                Column.physical("b", ARRAY(BIGINT().notNull()).notNull()),
                                Column.physical("c", MAP(BIGINT(), STRING()).notNull()),
                                Column.physical("d", MULTISET(BIGINT().notNull()).notNull()),
                                Column.physical("e", ROW(FIELD("a", BIGINT())).notNull()),
                                Column.physical(
                                        "f", RAW(Void.class, VoidSerializer.INSTANCE).notNull()),
                                Column.physical("g", BYTES().notNull())),
                        Collections.emptyList(),
                        UniqueConstraint.primaryKey(
                                "name", Arrays.asList("a", "b", "c", "d", "e", "f", "g")));

        assertThatThrownBy(
                        () ->
                                sinkFactory.createDynamicTableSink(
                                        createPrefilledTestContext()
                                                .withSchema(resolvedSchema)
                                                .build()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "The table has a primary key on columns of illegal types: "
                                + "[ARRAY, MAP, MULTISET, ROW, RAW, VARBINARY].");
    }

    @Test
    public void validateWrongCredential() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();

        assertThatThrownBy(
                        () ->
                                sinkFactory.createDynamicTableSink(
                                        createPrefilledTestContext()
                                                .withOption(
                                                        ElasticsearchConnectorOptions
                                                                .USERNAME_OPTION
                                                                .key(),
                                                        "username")
                                                .withOption(
                                                        ElasticsearchConnectorOptions
                                                                .PASSWORD_OPTION
                                                                .key(),
                                                        "")
                                                .build()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "'username' and 'password' must be set at the same time. Got: username 'username' and password ''");
    }

    @Test
    public void validateDynamicIndexOnChangelogStream() {
        ElasticsearchDynamicSinkFactoryBase sinkFactory = createSinkFactory();
        DynamicTableSink sink =
                sinkFactory.createDynamicTableSink(
                        createPrefilledTestContext()
                                .withOption(
                                        ElasticsearchConnectorOptions.INDEX_OPTION.key(),
                                        "dynamic-index-{now()|yyyy-MM-dd}_index")
                                .build());

        ChangelogMode changelogMode =
                ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.DELETE)
                        .addContainedKind(RowKind.INSERT)
                        .build();
        assertThatThrownBy(() -> sink.getChangelogMode(changelogMode))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Dynamic indexing based on system time only works on append only stream.");
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
        SinkV2Provider provider =
                (SinkV2Provider) esSink.getSinkRuntimeProvider(new ElasticsearchUtil.MockContext());
        assertThat(provider.getParallelism()).hasValue(2);
    }
}
