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
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
 *         .fields(names, dataTypes)
 *         .build())
 *   .withFormat(
 *     new Json()
 *       .jsonSchema("{...}")
 *       .failOnMissingField(false))
 *   .withComment("test comment")
 *   .build()
 * </code>
 */
@PublicEvolving
public final class CatalogTableBuilder extends TableDescriptor<CatalogTableBuilder> {

	private final TableSchema tableSchema;

	private String comment;

	private final boolean isGeneric;

	private List<String> partitionKeys = new ArrayList<>();

	private Map<String, String> properties = Collections.emptyMap();

	public CatalogTableBuilder(ConnectorDescriptor connectorDescriptor, TableSchema tableSchema) {
		super(connectorDescriptor);
		this.tableSchema = Preconditions.checkNotNull(tableSchema);

		// We don't support non-generic table currently
		this.isGeneric = true;
	}

	public CatalogTableBuilder withComment(String comment) {
		this.comment = Preconditions.checkNotNull(comment, "Comment must not be null.");
		return this;
	}

	public CatalogTableBuilder withProperties(Map<String, String> properties) {
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		return this;
	}

	public CatalogTableBuilder withPartitionKeys(List<String> partitionKeys) {
		this.partitionKeys = Preconditions.checkNotNull(partitionKeys, "PartitionKeys must not be null.");
		return this;
	}

	/**
	 * Builds a {@link CatalogTable}.
	 */
	public CatalogTable build() {
		return new CatalogTableImpl(
			tableSchema,
			partitionKeys,
			toProperties(),
			comment);
	}

	@Override
	protected Map<String, String> additionalProperties() {
		DescriptorProperties descriptorProperties = new DescriptorProperties();

		descriptorProperties.putBoolean(CatalogConfig.IS_GENERIC, isGeneric);

		descriptorProperties.putProperties(this.properties);

		return descriptorProperties.asMap();
	}
}
