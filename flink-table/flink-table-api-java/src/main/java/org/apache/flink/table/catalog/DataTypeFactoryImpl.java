/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasNestedRoot;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Implementation of a {@link DataTypeFactory}.
 */
@Internal
final class DataTypeFactoryImpl implements DataTypeFactory {

	private final ClassLoader classLoader;

	private final Supplier<ExecutionConfig> executionConfig;

	DataTypeFactoryImpl(
			ClassLoader classLoader,
			ReadableConfig config,
			@Nullable ExecutionConfig executionConfig) {
		this.classLoader = classLoader;
		this.executionConfig = createKryoExecutionConfig(classLoader, config, executionConfig);
	}

	@Override
	public Optional<DataType> createDataType(String name) {
		final LogicalType parsedType = LogicalTypeParser.parse(name, classLoader);
		if (hasNestedRoot(parsedType, LogicalTypeRoot.UNRESOLVED)) {
			throw new TableException("User-defined types are not supported yet.");
		}
		return Optional.of(fromLogicalToDataType(parsedType));
	}

	@Override
	public Optional<DataType> createDataType(UnresolvedIdentifier identifier) {
		if (!identifier.getDatabaseName().isPresent()) {
			return createDataType(identifier.getObjectName());
		}
		throw new TableException("User-defined types are not supported yet.");
	}

	@Override
	public <T> DataType createRawDataType(Class<T> clazz) {
		return DataTypes.RAW(clazz, new KryoSerializer<>(clazz, executionConfig.get()));
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a lazy {@link ExecutionConfig} for the {@link KryoSerializer} with information from
	 * existing {@link ExecutionConfig} (if available) enriched with table {@link ReadableConfig}.
	 */
	private static Supplier<ExecutionConfig> createKryoExecutionConfig(
			ClassLoader classLoader,
			ReadableConfig config,
			ExecutionConfig executionConfig) {
		return () -> {
			final ExecutionConfig newExecutionConfig = new ExecutionConfig();

			if (executionConfig != null) {
				executionConfig.getDefaultKryoSerializers()
					.forEach((c, s) -> newExecutionConfig.addDefaultKryoSerializer(c, s.getSerializer()));

				executionConfig.getDefaultKryoSerializerClasses()
					.forEach(newExecutionConfig::addDefaultKryoSerializer);

				executionConfig.getRegisteredKryoTypes()
					.forEach(newExecutionConfig::registerKryoType);

				executionConfig.getRegisteredTypesWithKryoSerializerClasses()
					.forEach(newExecutionConfig::registerTypeWithKryoSerializer);

				executionConfig.getRegisteredTypesWithKryoSerializers()
					.forEach((c, s) -> newExecutionConfig.registerTypeWithKryoSerializer(c, s.getSerializer()));
			}

			newExecutionConfig.configure(config, classLoader);

			return newExecutionConfig;
		};
	}
}
