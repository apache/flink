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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ConnectorFormatDescriptor;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Metadata;
import org.apache.flink.table.descriptors.Statistics;
import org.apache.flink.table.descriptors.StreamableDescriptor;
import org.apache.flink.table.descriptors.TableDescriptor;

import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_RETRACT;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_UPSERT;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A builder for creating a {@link CatalogTable}.
 *
 * <p>It takes {@link Descriptor}s which allow for declaring the communication to external
 * systems in an implementation-agnostic way. The classpath is scanned for suitable table
 * factories that match the desired configuration.
 *
 * <p>Use the provided builder methods to configure the catalog table accordingly.
 *
 * <p>The following example shows how to read from a connector using a JSON format and
 * declaring it as a table source:
 *
 * <code>
 * CatalogTable table = new CatalogTableBuilder(
 *       new ExternalSystemXYZ()
 *         .version("0.11"),
 *       new TableSchema.Builder()
 *     	   .fields(names, dataTypes)
 *     	   .build())
 *   .withFormat(
 *     new Json()
 *       .jsonSchema("{...}")
 *       .failOnMissingField(false))
 *   .withComment("test comment")
 *   .build()
 * </code>
 */
@PublicEvolving
public class CatalogTableBuilder
		extends TableDescriptor
		implements ConnectorFormatDescriptor<CatalogTableBuilder>, StreamableDescriptor<CatalogTableBuilder> {

	private final ConnectorDescriptor connectorDescriptor;
	private final TableSchema tableSchema;

	private String comment;

	private Optional<FormatDescriptor> formatDescriptor = Optional.empty();
	private Optional<Statistics> statisticsDescriptor = Optional.empty();
	private Optional<Metadata> metadataDescriptor = Optional.empty();
	private Optional<String> updateMode = Optional.empty();

	public CatalogTableBuilder(ConnectorDescriptor connectorDescriptor, TableSchema tableSchema) {
		this.connectorDescriptor = checkNotNull(connectorDescriptor);
		this.tableSchema = checkNotNull(tableSchema);
	}

	@Override
	public CatalogTableBuilder withFormat(FormatDescriptor format) {
		this.formatDescriptor = Optional.of(checkNotNull(format));
		return this;
	}

	@Override
	public CatalogTableBuilder inAppendMode() {
		updateMode = Optional.of(UPDATE_MODE_VALUE_APPEND);
		return this;
	}

	@Override
	public CatalogTableBuilder inRetractMode() {
		updateMode = Optional.of(UPDATE_MODE_VALUE_RETRACT);
		return this;
	}

	@Override
	public CatalogTableBuilder inUpsertMode() {
		updateMode = Optional.of(UPDATE_MODE_VALUE_UPSERT);
		return this;
	}

	public CatalogTableBuilder withComment(String comment) {
		this.comment = comment;
		return this;
	}

	/**
	 * Build a {@link CatalogTable}.
	 *
	 * @return catalog table
	 */
	public CatalogTable build() {
		return new CatalogTableImpl(
			tableSchema,
			toProperties(),
			comment);
	}

	@Override
	public Map<String, String> toProperties() {
		DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(connectorDescriptor.toProperties());

		if (formatDescriptor.isPresent()) {
			properties.putProperties(formatDescriptor.get().toProperties());
		}

		if (statisticsDescriptor.isPresent()) {
			properties.putProperties(statisticsDescriptor.get().toProperties());
		}

		if (metadataDescriptor.isPresent()) {
			properties.putProperties(metadataDescriptor.get().toProperties());
		}

		if (updateMode.isPresent()) {
			properties.putString(UPDATE_MODE, updateMode.get());
		}

		return properties.asMap();
	}
}
