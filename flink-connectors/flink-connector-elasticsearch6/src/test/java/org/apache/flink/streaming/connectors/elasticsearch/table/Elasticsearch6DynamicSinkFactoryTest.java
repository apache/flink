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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.flink.streaming.connectors.elasticsearch.table.TestContext.context;

/**
 * Tests for validation in {@link Elasticsearch6DynamicSinkFactory}.
 */
public class Elasticsearch6DynamicSinkFactoryTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void validateEmptyConfiguration() {
		Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"One or more required options are missing.\n" +
				"\n" +
				"Missing required options are:\n" +
				"\n" +
				"document-type\n" +
				"hosts\n" +
				"index");
		sinkFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.build()
		);
	}

	@Test
	public void validateWrongIndex() {
		Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'index' must not be empty");
		sinkFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption("index", "")
				.withOption("document-type", "MyType")
				.withOption("hosts", "http://localhost:12345")
				.build()
		);
	}

	@Test
	public void validateWrongHosts() {
		Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"Could not parse host 'wrong-host' in option 'hosts'. It should follow the format 'http://host_name:port'.");
		sinkFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption("index", "MyIndex")
				.withOption("document-type", "MyType")
				.withOption("hosts", "wrong-host")
				.build()
		);
	}

	@Test
	public void validateWrongFlushSize() {
		Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'sink.bulk-flush.max-size' must be at least 1mb character. Got: 1024 bytes");
		sinkFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.DOCUMENT_TYPE_OPTION.key(), "MyType")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.BULK_FLASH_MAX_SIZE_OPTION.key(), "1kb")
				.build()
		);
	}

	@Test
	public void validateWrongRetries() {
		Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'sink.bulk-flush.backoff.max-retries' must be at least 1. Got: 0");
		sinkFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.DOCUMENT_TYPE_OPTION.key(), "MyType")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION.key(), "0")
				.build()
		);
	}

	@Test
	public void validateWrongMaxActions() {
		Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'sink.bulk-flush.max-actions' must be at least 1 character. Got: 0");
		sinkFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.DOCUMENT_TYPE_OPTION.key(), "MyType")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.BULK_FLUSH_MAX_ACTIONS_OPTION.key(), "0")
				.build()
		);
	}

	@Test
	public void validateWrongBackoffDelay() {
		Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"Invalid value for option 'sink.bulk-flush.backoff.delay'.");
		sinkFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.DOCUMENT_TYPE_OPTION.key(), "MyType")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION.key(), "-1s")
				.build()
		);
	}

	@Test
	public void validatePrimaryKeyOnIllegalColumn() {
		Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"The table has a primary key on columns of illegal types: " +
				"[ARRAY, MAP, MULTISET, ROW, RAW, VARBINARY].\n" +
				" Elasticsearch sink does not support primary keys on columns of types: " +
				"[ARRAY, MAP, MULTISET, STRUCTURED_TYPE, ROW, RAW, BINARY, VARBINARY].");
		sinkFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.BIGINT().notNull())
					.field("b", DataTypes.ARRAY(DataTypes.BIGINT().notNull()).notNull())
					.field("c", DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()).notNull())
					.field("d", DataTypes.MULTISET(DataTypes.BIGINT().notNull()).notNull())
					.field("e", DataTypes.ROW(DataTypes.FIELD("a", DataTypes.BIGINT())).notNull())
					.field("f", DataTypes.RAW(Types.BIG_INT).notNull())
					.field("g", DataTypes.BYTES().notNull())
					.primaryKey("a", "b", "c", "d", "e", "f", "g")
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION.key(), "1s")
				.build()
		);
	}
}
