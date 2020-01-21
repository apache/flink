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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.StructuredType;

import java.util.Optional;

/**
 * Factory for creating fully resolved data types that can be used for planning.
 *
 * <p>The factory is useful for types that cannot be created with one of the static methods in
 * {@link DataTypes}) because they require access to configuration or catalog.
 */
@PublicEvolving
public interface DataTypeFactory {

	/**
	 * Creates a type by a fully or partially defined name.
	 *
	 * <p>The factory will parse and resolve the name of a type to a {@link DataType}. This includes
	 * both built-in types as well as user-defined types (see {@link DistinctType} and {@link StructuredType}).
	 */
	Optional<DataType> createDataType(String name);

	/**
	 * Creates a type by a fully or partially defined identifier.
	 *
	 * <p>The factory will parse and resolve the name of a type to a {@link DataType}. This includes
	 * both built-in types as well as user-defined types (see {@link DistinctType} and {@link StructuredType}).
	 */
	Optional<DataType> createDataType(UnresolvedIdentifier identifier);

	/**
	 * Creates a type by analyzing the given class.
	 *
	 * <p>It does this by using Java reflection which can be supported by {@link DataTypeHint} annotations
	 * for nested, structured types.
	 *
	 * <p>It will throw an {@link ValidationException} in cases where the reflective extraction needs
	 * more information or simply fails.
	 *
	 * <p>The following examples show how to use and enrich the extraction process:
	 *
	 * <pre>
	 * {@code
	 *   // returns INT
	 *   createDataType(Integer.class)
	 *
	 *   // returns TIMESTAMP(9)
	 *   createDataType(java.time.LocalDateTime.class)
	 *
	 *   // returns an anonymous, unregistered structured type
	 *   // that is deeply integrated into the API compared to opaque RAW types
	 *   class User {
	 *
	 *     // extract fields automatically
	 *     public String name;
	 *     public int age;
	 *
	 *     // enrich the extraction with precision information
	 *     public @DataTypeHint("DECIMAL(10,2)") BigDecimal accountBalance;
	 *
	 *     // enrich the extraction with forcing using RAW types
	 *     public @DataTypeHint(forceRawPattern = "scala.") Address address;
	 *
	 *     // enrich the extraction by specifying defaults
	 *     public @DataTypeHint(defaultSecondPrecision = 3) Log log;
	 *   }
	 *   createDataType(User.class)
	 * }
	 * </pre>
	 */
	<T> DataType createDataType(Class<T> clazz);

	/**
	 * Creates a RAW type for the given class in cases where no serializer is known and a generic serializer
	 * should be used. The factory will create {@link DataTypes#RAW(Class, TypeSerializer)} with Flink's
	 * default RAW serializer that is automatically configured.
	 *
	 * <p>Note: This type is a black box within the table ecosystem and is only deserialized at the edges
	 * of the API.
	 */
	<T> DataType createRawDataType(Class<T> clazz);
}
