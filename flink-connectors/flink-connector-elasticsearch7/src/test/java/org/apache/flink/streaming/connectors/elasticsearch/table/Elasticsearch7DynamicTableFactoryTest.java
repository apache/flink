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
 * Tests for validation in {@link Elasticsearch7DynamicTableFactory}.
 */
public class Elasticsearch7DynamicTableFactoryTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void validateEmptyConfiguration() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"One or more required options are missing.\n" +
				"\n" +
				"Missing required options are:\n" +
				"\n" +
				"hosts\n" +
				"index");
		tableFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.build()
		);
	}

	@Test
	public void validateWrongIndex() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'index' must not be empty");
		tableFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption("index", "")
				.withOption("hosts", "http://localhost:12345")
				.build()
		);
	}

	@Test
	public void validateWrongHosts() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"Could not parse host 'wrong-host' in option 'hosts'. It should follow the format 'http://host_name:port'.");
		tableFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption("index", "MyIndex")
				.withOption("hosts", "wrong-host")
				.build()
		);
	}

	@Test
	public void validateWrongScrollMaxSize() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'scan.scroll.max-size' must be at least 1. Got: 0");
		tableFactory.createDynamicTableSource(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.SCROLL_MAX_SIZE_OPTION.key(), "0")
				.build()
		);
	}

	@Test
	public void validateWrongScrollTimeout() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'scan.scroll.timeout' must be at least 1. Got: 0");
		tableFactory.createDynamicTableSource(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.SCROLL_TIMEOUT_OPTION.key(), "0")
				.build()
		);
	}

	@Test
	public void validateWrongCacheMaxSize() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'lookup.cache.max-rows' must be at least 1. Got: 0");
		tableFactory.createDynamicTableSource(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.LOOKUP_CACHE_MAX_ROWS.key(), "0")
				.build()
		);
	}

	@Test
	public void validateWrongCacheTTL() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'lookup.cache.ttl' must be at least 1. Got: 0");
		tableFactory.createDynamicTableSource(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.LOOKUP_CACHE_TTL.key(), "0")
				.build()
		);
	}

	@Test
	public void validateWrongMaxRetries() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'lookup.max-retries' must be at least 1. Got: 0");
		tableFactory.createDynamicTableSource(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.LOOKUP_MAX_RETRIES.key(), "0")
				.build()
		);
	}

	@Test
	public void validateWrongFlushSize() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'sink.bulk-flush.max-size' must be in MB granularity. Got: 1024 bytes");
		tableFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.BULK_FLASH_MAX_SIZE_OPTION.key(), "1kb")
				.build()
		);
	}

	@Test
	public void validateWrongRetries() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'sink.bulk-flush.backoff.max-retries' must be at least 1. Got: 0");
		tableFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION.key(), "0")
				.build()
		);
	}

	@Test
	public void validateWrongMaxActions() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"'sink.bulk-flush.max-actions' must be at least 1 character. Got: -2");
		tableFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.BULK_FLUSH_MAX_ACTIONS_OPTION.key(), "-2")
				.build()
		);
	}

	@Test
	public void validateWrongBackoffDelay() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"Invalid value for option 'sink.bulk-flush.backoff.delay'.");
		tableFactory.createDynamicTableSink(
			context()
				.withSchema(TableSchema.builder()
					.field("a", DataTypes.TIME())
					.build())
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), "MyIndex")
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://localhost:1234")
				.withOption(ElasticsearchOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION.key(), "-1s")
				.build()
		);
	}

	@Test
	public void validatePrimaryKeyOnIllegalColumn() {
		Elasticsearch7DynamicTableFactory tableFactory = new Elasticsearch7DynamicTableFactory();

		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"The table has a primary key on columns of illegal types: " +
				"[ARRAY, MAP, MULTISET, ROW, RAW, VARBINARY].\n" +
				" Elasticsearch sink does not support primary keys on columns of types: " +
				"[ARRAY, MAP, MULTISET, STRUCTURED_TYPE, ROW, RAW, BINARY, VARBINARY].");
		tableFactory.createDynamicTableSink(
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
