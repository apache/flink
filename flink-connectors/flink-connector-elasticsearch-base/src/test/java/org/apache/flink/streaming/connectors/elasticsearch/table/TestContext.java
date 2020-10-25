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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A utility class for mocking {@link DynamicTableFactory.Context}.
 */
class TestContext {
	private TableSchema schema;
	private Map<String, String> properties = new HashMap<>();

	public static TestContext context() {
		return new TestContext();
	}

	public TestContext withSchema(TableSchema schema) {
		this.schema = schema;
		return this;
	}

	DynamicTableFactory.Context build() {
		return new DynamicTableFactory.Context() {
			@Override
			public ObjectIdentifier getObjectIdentifier() {
				return null;
			}

			@Override
			public CatalogTable getCatalogTable() {
				return new CatalogTableImpl(
					schema,
					properties,
					""
				);
			}

			@Override
			public ReadableConfig getConfiguration() {
				return null;
			}

			@Override
			public ClassLoader getClassLoader() {
				return TestContext.class.getClassLoader();
			}

			@Override
			public boolean isTemporary() {
				return false;
			}
		};
	}

	public TestContext withOption(String key, String value) {
		properties.put(key, value);
		return this;
	}
}
