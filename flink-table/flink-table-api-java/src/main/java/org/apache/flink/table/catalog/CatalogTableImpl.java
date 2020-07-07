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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A catalog table implementation.
 */
public class CatalogTableImpl extends AbstractCatalogTable {

	public CatalogTableImpl(
			TableSchema tableSchema,
			Map<String, String> properties,
			String comment) {
		this(tableSchema, new ArrayList<>(), properties, comment);
	}

	public CatalogTableImpl(
			TableSchema tableSchema,
			List<String> partitionKeys,
			Map<String, String> properties,
			String comment) {
		super(tableSchema, partitionKeys, properties, comment);
	}

	@Override
	public CatalogBaseTable copy() {
		return new CatalogTableImpl(
			getSchema().copy(),
			new ArrayList<>(getPartitionKeys()),
			new HashMap<>(getProperties()),
			getComment());
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.of(getComment());
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.of("This is a catalog table in an im-memory catalog");
	}

	@Override
	public Map<String, String> toProperties() {
		DescriptorProperties descriptor = new DescriptorProperties();

		descriptor.putTableSchema(Schema.SCHEMA, getSchema());
		descriptor.putPartitionKeys(getPartitionKeys());

		Map<String, String> properties = new HashMap<>(getProperties());
		properties.remove(CatalogConfig.IS_GENERIC);

		descriptor.putProperties(properties);

		return descriptor.asMap();
	}

	@Override
	public CatalogTable copy(Map<String, String> options) {
		return new CatalogTableImpl(getSchema(), getPartitionKeys(), options, getComment());
	}

	/**
	 * Construct a {@link CatalogTableImpl} from complete properties that contains table schema.
	 */
	public static CatalogTableImpl fromProperties(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(properties);
		TableSchema tableSchema = descriptorProperties.getTableSchema(Schema.SCHEMA);
		List<String> partitionKeys = descriptorProperties.getPartitionKeys();
		return new CatalogTableImpl(
				tableSchema,
				partitionKeys,
				removeRedundant(properties, tableSchema, partitionKeys),
				""
		);
	}

	/**
	 * Construct catalog table properties from {@link #toProperties()}.
	 */
	public static Map<String, String> removeRedundant(
			Map<String, String> properties,
			TableSchema schema,
			List<String> partitionKeys) {
		Map<String, String> ret = new HashMap<>(properties);
		DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putTableSchema(Schema.SCHEMA, schema);
		descriptorProperties.putPartitionKeys(partitionKeys);
		descriptorProperties.asMap().keySet().forEach(ret::remove);
		return ret;
	}
}
