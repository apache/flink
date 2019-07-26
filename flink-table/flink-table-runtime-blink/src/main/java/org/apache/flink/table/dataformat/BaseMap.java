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

package org.apache.flink.table.dataformat;

import org.apache.flink.table.types.logical.LogicalType;

import java.util.Map;

/**
 * An interface for map used internally in Flink Table/SQL.
 *
 * <p>There are different implementations depending on the scenario:
 * After serialization, it becomes the {@link BinaryMap} format.
 * Convenient updates use the {@link GenericMap} format.
 */
public interface BaseMap {

	/**
	 * Invoke by codeGens.
	 */
	int numElements();

	/**
	 * This method will return a Java map containing INTERNAL type data.
	 * If you want a Java map containing external type data, you have to use converters.
	 */
	Map toJavaMap(LogicalType keyType, LogicalType valueType);

	// NOTE:
	//
	// As binary map has specific `get` and `toString` method,
	// we do not provide these methods in the interface.
	// Instead, we implement them in codegen.
	//
	// `get` is implemented in ScalarOperatorGens -> generateMapGet()
	// `toString` is implemented in ScalarOperatorGens -> generateCast()
}
