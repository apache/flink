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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * Describes a table connected from {@link TableEnvironment#connect(ConnectorDescriptor)}.
 *
 * <p>It can access {@link TableEnvironment} for fluently registering the table.
 */
@PublicEvolving
public abstract class ConnectTableDescriptor
	extends TableDescriptor<ConnectTableDescriptor>
	implements SchematicDescriptor<ConnectTableDescriptor> {

	private final TableEnvironment tableEnv;

	private @Nullable Schema schemaDescriptor;

	public ConnectTableDescriptor(TableEnvironment tableEnv, ConnectorDescriptor connectorDescriptor) {
		super(connectorDescriptor);
		this.tableEnv = tableEnv;
	}

	/**
	 * Specifies the resulting table schema.
	 */
	@Override
	public ConnectTableDescriptor withSchema(Schema schema) {
		schemaDescriptor = Preconditions.checkNotNull(schema, "Schema must not be null.");
		return this;
	}

	/**
	 * Searches for the specified table source, configures it accordingly, and registers it as
	 * a table under the given name.
	 *
	 * @param name table name to be registered in the table environment
	 */
	public void registerTableSource(String name) {
		Preconditions.checkNotNull(name);
		TableSource<?> tableSource = TableFactoryUtil.findAndCreateTableSource(this);
		tableEnv.registerTableSource(name, tableSource);
	}

	/**
	 * Searches for the specified table sink, configures it accordingly, and registers it as
	 * a table under the given name.
	 *
	 * @param name table name to be registered in the table environment
	 */
	public void registerTableSink(String name) {
		Preconditions.checkNotNull(name);
		TableSink<?> tableSink = TableFactoryUtil.findAndCreateTableSink(this);
		tableEnv.registerTableSink(name, tableSink);
	}

	/**
	 * Searches for the specified table source and sink, configures them accordingly, and registers
	 * them as a table under the given name.
	 *
	 * @param name table name to be registered in the table environment
	 */
	public void registerTableSourceAndSink(String name) {
		registerTableSource(name);
		registerTableSink(name);
	}

	@Override
	protected Map<String, String> additionalProperties() {
		if (schemaDescriptor != null) {
			return schemaDescriptor.toProperties();
		}
		return Collections.emptyMap();
	}
}
