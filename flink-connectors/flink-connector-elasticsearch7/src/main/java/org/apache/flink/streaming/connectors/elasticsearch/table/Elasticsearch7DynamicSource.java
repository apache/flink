/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticSearch7InputFormat;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.TableSchemaUtils;

/**
 * A {@link DynamicTableSource} that describes how to create a {@link Elasticsearch7DynamicSource} from a logical
 * description.
 */
@Internal
public class Elasticsearch7DynamicSource implements ScanTableSource, SupportsProjectionPushDown {

	private final DecodingFormat<DeserializationSchema<RowData>> format;
	private final Elasticsearch7Configuration config;
	//	projectedFields 不使用用TableSchema替代
	private TableSchema physicalSchema;

	public Elasticsearch7DynamicSource(
		DecodingFormat<DeserializationSchema<RowData>> format,
		Elasticsearch7Configuration config,
		TableSchema physicalSchema) {
		this.format = format;
		this.config = config;
		this.physicalSchema = physicalSchema;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		ElasticSearch7InputFormat.Builder elasticsearchInputformatBuilder = new ElasticSearch7InputFormat.Builder();
		elasticsearchInputformatBuilder.setHttpHosts(config.getHosts());

		RestClientFactory restClientFactory = null;
		if (config.getPathPrefix().isPresent()) {
			restClientFactory = new Elasticsearch7DynamicSink.DefaultRestClientFactory(config.getPathPrefix().get());
		} else {
			restClientFactory = restClientBuilder -> {
			};
		}

		elasticsearchInputformatBuilder.setRestClientFactory(restClientFactory);
		elasticsearchInputformatBuilder.setDeserializationSchema(this.format.createRuntimeDecoder(runtimeProviderContext, physicalSchema.toRowDataType()));
		elasticsearchInputformatBuilder.setFieldNames(physicalSchema.getFieldNames());
		elasticsearchInputformatBuilder.setRowDataTypeInfo((TypeInformation<RowData>) runtimeProviderContext
			.createTypeInformation(physicalSchema.toRowDataType()));
		elasticsearchInputformatBuilder.setIndex(config.getIndex());
		config.getScrollMaxSize().ifPresent(elasticsearchInputformatBuilder::setScrollMaxSize);
		config.getScrollTimeout().ifPresent(elasticsearchInputformatBuilder::setScrollTimeout);

//		for SupportsFilterPushDown/ SupportsLimitPushDown
//		builder.setPredicate();
//		builder.setLimit();

		return InputFormatProvider.of(
			elasticsearchInputformatBuilder.build()
		);
	}

	@Override
	public DynamicTableSource copy() {
		return new Elasticsearch7DynamicSource(format, config, physicalSchema);
	}

	@Override
	public String asSummaryString() {
		return "Elasticsearch-7";
	}

	@Override
	public boolean supportsNestedProjection() {
		return false;
	}

	@Override
	public void applyProjection(int[][] projectedFields) {
		this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
	}
}
