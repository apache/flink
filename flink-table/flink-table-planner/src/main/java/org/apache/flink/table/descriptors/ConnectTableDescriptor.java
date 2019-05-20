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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Optional;

/**
 * Common class for table's created with {@link TableEnvironment#connect(ConnectorDescriptor connectorDescriptor)}.
 */
@PublicEvolving
public abstract class ConnectTableDescriptor<D extends ConnectTableDescriptor<D>>
	extends TableDescriptor
	implements SchematicDescriptor<D>, RegistrableDescriptor {

	private Optional<FormatDescriptor> formatDescriptor = Optional.empty();
	private Optional<Schema> schemaDescriptor = Optional.empty();

	private final TableEnvironment tableEnv;
	private final ConnectorDescriptor connectorDescriptor;

	public ConnectTableDescriptor(TableEnvironment tableEnv, ConnectorDescriptor connectorDescriptor) {
		this.tableEnv = tableEnv;
		this.connectorDescriptor = connectorDescriptor;
	}

	/**
	 * Searches for the specified table source, configures it accordingly, and registers it as
	 * a table under the given name.
	 *
	 * @param name table name to be registered in the table environment
	 */
	@Override
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
	@Override
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
	@Override
	public void registerTableSourceAndSink(String name) {
		registerTableSource(name);
		registerTableSink(name);
	}

	/**
	 * Specifies the format that defines how to read data from a connector.
	 */
	@Override
	public D withFormat(FormatDescriptor format) {
		formatDescriptor = Optional.of(Preconditions.checkNotNull(format));
		return (D) this;
	}

	/**
	 * Specifies the resulting table schema.
	 */
	@Override
	public D withSchema(Schema schema) {
		schemaDescriptor = Optional.of(Preconditions.checkNotNull(schema));
		return (D) this;
	}

	/**
	 * Converts this descriptor into a set of properties.
	 */
	@Override
	public Map<String, String> toProperties() {
		DescriptorProperties properties = new DescriptorProperties();

		// this performs only basic validation
		// more validation can only happen within a factory
		if (connectorDescriptor.isFormatNeeded() && !formatDescriptor.isPresent()) {
			throw new ValidationException(String.format("The connector %s requires a format description.", connectorDescriptor.toString()));
		} else if (!connectorDescriptor.isFormatNeeded() && formatDescriptor.isPresent()) {
			throw new ValidationException(
				String.format("The connector %s does not require a format description " +
				"but %s found.", connectorDescriptor.toString(), formatDescriptor.get().toString()));
		}

		properties.putProperties(connectorDescriptor.toProperties());

		formatDescriptor.ifPresent(s -> properties.putProperties(s.toProperties()));
		schemaDescriptor.ifPresent(s -> properties.putProperties(s.toProperties()));

		return properties.asMap();
	}
}
