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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Operation to describe a ALTER FUNCTION statement for temporary catalog function.
 */
public class AlterFunctionOperation implements AlterOperation  {
	private final ObjectIdentifier functionIdentifier;
	private CatalogFunction catalogFunction;
	private boolean ifExists;

	public AlterFunctionOperation(
			ObjectIdentifier functionIdentifier,
			CatalogFunction catalogFunction,
			boolean ifExists) {
		this.functionIdentifier = functionIdentifier;
		this.catalogFunction = catalogFunction;
		this.ifExists = ifExists;
	}

	public CatalogFunction getCatalogFunction() {
		return this.catalogFunction;
	}

	public ObjectIdentifier getFunctionIdentifier() {
		return this.functionIdentifier;
	}

	public boolean isIfExists() {
		return this.ifExists;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("catalogFunction", catalogFunction.getDetailedDescription());
		params.put("identifier", functionIdentifier);
		params.put("ifExists", ifExists);

		return OperationUtils.formatWithChildren(
			"ALTER FUNCTION",
			params,
			Collections.emptyList(),
			Operation::asSummaryString);
	}

	public String getFunctionName() {
		return this.functionIdentifier.getObjectName();
	}
}
