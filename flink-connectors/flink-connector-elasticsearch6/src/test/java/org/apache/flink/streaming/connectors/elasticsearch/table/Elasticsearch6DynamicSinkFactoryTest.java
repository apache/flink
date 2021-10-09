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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.streaming.connectors.elasticsearch.table.TestContext.context;

/** Tests for validation in {@link Elasticsearch6DynamicSinkFactory}. */
public class Elasticsearch6DynamicSinkFactoryTest extends TestLogger {
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void validateEmptyConfiguration() {
        Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

        thrown.expect(ValidationException.class);
        thrown.expectMessage(
                "One or more required options are missing.\n"
                        + "\n"
                        + "Missing required options are:\n"
                        + "\n"
                        + "document-type\n"
                        + "hosts\n"
                        + "index");
        sinkFactory.createDynamicTableSink(context().build());
    }

    @Test
    public void validateWrongIndex() {
        Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

        thrown.expect(ValidationException.class);
        thrown.expectMessage("'index' must not be empty");
        sinkFactory.createDynamicTableSink(
                context()
                        .withOption("index", "")
                        .withOption("document-type", "MyType")
                        .withOption("hosts", "http://localhost:12345")
                        .build());
    }

    @Test
    public void validateWrongHosts() {
        Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

        thrown.expect(ValidationException.class);
        thrown.expectMessage(
                "Could not parse host 'wrong-host' in option 'hosts'. It should follow the format 'http://host_name:port'.");
        sinkFactory.createDynamicTableSink(
                context()
                        .withOption("index", "MyIndex")
                        .withOption("document-type", "MyType")
                        .withOption("hosts", "wrong-host")
                        .build());
    }

    @Test
    public void validateWrongFlushSize() {
        Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

        thrown.expect(ValidationException.class);
        thrown.expectMessage(
                "'sink.bulk-flush.max-size' must be in MB granularity. Got: 1024 bytes");
        sinkFactory.createDynamicTableSink(
                context()
                        .withOption(ElasticsearchConnectorOptions.INDEX_OPTION.key(), "MyIndex")
                        .withOption(
                                ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION.key(), "MyType")
                        .withOption(
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                "http://localhost:1234")
                        .withOption(
                                ElasticsearchConnectorOptions.BULK_FLASH_MAX_SIZE_OPTION.key(),
                                "1kb")
                        .build());
    }

    @Test
    public void validateWrongRetries() {
        Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

        thrown.expect(ValidationException.class);
        thrown.expectMessage("'sink.bulk-flush.backoff.max-retries' must be at least 1. Got: 0");
        sinkFactory.createDynamicTableSink(
                context()
                        .withOption(ElasticsearchConnectorOptions.INDEX_OPTION.key(), "MyIndex")
                        .withOption(
                                ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION.key(), "MyType")
                        .withOption(
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                "http://localhost:1234")
                        .withOption(
                                ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION
                                        .key(),
                                "0")
                        .build());
    }

    @Test
    public void validateWrongMaxActions() {
        Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

        thrown.expect(ValidationException.class);
        thrown.expectMessage("'sink.bulk-flush.max-actions' must be at least 1. Got: -2");
        sinkFactory.createDynamicTableSink(
                context()
                        .withOption(ElasticsearchConnectorOptions.INDEX_OPTION.key(), "MyIndex")
                        .withOption(
                                ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION.key(), "MyType")
                        .withOption(
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                "http://localhost:1234")
                        .withOption(
                                ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION.key(),
                                "-2")
                        .build());
    }

    @Test
    public void validateWrongBackoffDelay() {
        Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

        thrown.expect(ValidationException.class);
        thrown.expectMessage("Invalid value for option 'sink.bulk-flush.backoff.delay'.");
        sinkFactory.createDynamicTableSink(
                context()
                        .withOption(ElasticsearchConnectorOptions.INDEX_OPTION.key(), "MyIndex")
                        .withOption(
                                ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION.key(), "MyType")
                        .withOption(
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                "http://localhost:1234")
                        .withOption(
                                ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION.key(),
                                "-1s")
                        .build());
    }

    @Test
    public void validatePrimaryKeyOnIllegalColumn() {
        Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

        thrown.expect(ValidationException.class);
        thrown.expectMessage(
                "The table has a primary key on columns of illegal types: "
                        + "[ARRAY, MAP, MULTISET, ROW, RAW, VARBINARY].\n"
                        + " Elasticsearch sink does not support primary keys on columns of types: "
                        + "[ARRAY, MAP, MULTISET, STRUCTURED_TYPE, ROW, RAW, BINARY, VARBINARY].");
        sinkFactory.createDynamicTableSink(
                context()
                        .withSchema(
                                new ResolvedSchema(
                                        Arrays.asList(
                                                Column.physical("a", DataTypes.BIGINT().notNull()),
                                                Column.physical(
                                                        "b",
                                                        DataTypes.ARRAY(
                                                                        DataTypes.BIGINT()
                                                                                .notNull())
                                                                .notNull()),
                                                Column.physical(
                                                        "c",
                                                        DataTypes.MAP(
                                                                        DataTypes.BIGINT(),
                                                                        DataTypes.STRING())
                                                                .notNull()),
                                                Column.physical(
                                                        "d",
                                                        DataTypes.MULTISET(
                                                                        DataTypes.BIGINT()
                                                                                .notNull())
                                                                .notNull()),
                                                Column.physical(
                                                        "e",
                                                        DataTypes.ROW(
                                                                        DataTypes.FIELD(
                                                                                "a",
                                                                                DataTypes.BIGINT()))
                                                                .notNull()),
                                                Column.physical(
                                                        "f",
                                                        DataTypes.RAW(
                                                                        Void.class,
                                                                        VoidSerializer.INSTANCE)
                                                                .notNull()),
                                                Column.physical("g", DataTypes.BYTES().notNull())),
                                        Collections.emptyList(),
                                        UniqueConstraint.primaryKey(
                                                "name",
                                                Arrays.asList("a", "b", "c", "d", "e", "f", "g"))))
                        .withOption(ElasticsearchConnectorOptions.INDEX_OPTION.key(), "MyIndex")
                        .withOption(
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                "http://localhost:1234")
                        .withOption(
                                ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION.key(),
                                "1s")
                        .build());
    }

    @Test
    public void validateWrongCredential() {
        Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

        thrown.expect(ValidationException.class);
        thrown.expectMessage(
                "'username' and 'password' must be set at the same time. Got: username 'username' and password ''");
        sinkFactory.createDynamicTableSink(
                context()
                        .withOption(ElasticsearchConnectorOptions.INDEX_OPTION.key(), "MyIndex")
                        .withOption(
                                ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                                "http://localhost:1234")
                        .withOption(
                                ElasticsearchConnectorOptions.DOCUMENT_TYPE_OPTION.key(), "MyType")
                        .withOption(ElasticsearchConnectorOptions.USERNAME_OPTION.key(), "username")
                        .withOption(ElasticsearchConnectorOptions.PASSWORD_OPTION.key(), "")
                        .build());
    }
}
