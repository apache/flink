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

import java.util.Optional;

/**
 * Interface for a function in a catalog.
 */
public interface CatalogFunction {

	/**
	 * Get the full name of the class backing the function.
	 *
	 * @return the full name of the class
	 */
	String getClassName();

	/**
	 * Create a deep copy of the function.
	 *
	 * @return a deep copy of "this" instance
	 */
	CatalogFunction copy();

	/**
	 * Get a brief description of the function.
	 *
	 * @return an optional short description of the function
	 */
	Optional<String> getDescription();

	/**
	 * Get a detailed description of the function.
	 *
	 * @return an optional long description of the function
	 */
	Optional<String> getDetailedDescription();

	/**
	 * Distinguish if the function is a generic function.
	 *
	 * @return whether the function is a generic function
	 */
	boolean isGeneric();

	/**
	 * Distinguish if the function is a temporary function.
	 *
	 * @return whether the function is a generic function
	 */
	boolean isTemporary();

	/**
	 * Get the language used for the definition of function.
	 *
	 * @return  the language type of the function definition
	 */
	FunctionLanguage getFunctionLanguage();
}
