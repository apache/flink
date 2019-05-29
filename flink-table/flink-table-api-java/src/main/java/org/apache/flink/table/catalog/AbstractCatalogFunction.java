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

import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract catalog function implementation.
 */
public abstract class AbstractCatalogFunction implements CatalogFunction {
	private final String className; // Fully qualified class name of the function
	private final Map<String, String> properties;

	public AbstractCatalogFunction(String className, Map<String, String> properties) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(className), "className cannot be null or empty");

		this.className = className;
		this.properties = checkNotNull(properties, "properties cannot be null");
	}

	@Override
	public String getClassName() {
		return this.className;
	}

	@Override
	public Map<String, String> getProperties() {
		return this.properties;
	}

}
