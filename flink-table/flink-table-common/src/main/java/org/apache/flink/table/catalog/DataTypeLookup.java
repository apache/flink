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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.StructuredType;

import java.util.Optional;

/**
 * Catalog of types that can resolve the name of type to a {@link DataType}. This includes both
 * built-in logical types as well as user-defined logical types such as {@link DistinctType} and
 * {@link StructuredType}.
 */
@PublicEvolving
public interface DataTypeLookup {

	/**
	 * Lookup a type by a fully or partially defined name.
	 */
	Optional<DataType> lookupDataType(String name);

	/**
	 * Lookup a type by an unresolved identifier.
	 */
	Optional<DataType> lookupDataType(UnresolvedIdentifier identifier);

	/**
	 * Resolves a RAW type for the given class.
	 *
	 * <p>The {@link RawType} requires an instantiated serializer. Flink's default RAW serializer is
	 * configured during the resolution process.
	 */
	DataType resolveRawDataType(Class<?> clazz);
}
