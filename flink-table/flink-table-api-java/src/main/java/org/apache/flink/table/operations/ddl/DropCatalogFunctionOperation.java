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

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *  Operation to describe a DROP FUNCTION statement for catalog functions.
 */
public class DropCatalogFunctionOperation implements DropOperation {
	private final ObjectIdentifier functionIdentifier;
	private final boolean ifExists;
	private final boolean isTemporary;
	private final boolean isSystemFunction;

	public DropCatalogFunctionOperation(
			ObjectIdentifier functionIdentifier,
			boolean isTemporary,
			boolean isSystemFunction,
			boolean ifExists) {
		this.functionIdentifier = functionIdentifier;
		this.ifExists = ifExists;
		this.isTemporary = isTemporary;
		this.isSystemFunction = isSystemFunction;
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
		params.put("identifier", functionIdentifier);
		params.put("ifExists", ifExists);
		params.put("isSystemFunction", isSystemFunction);
		params.put("isTemporary", isTemporary);

		return OperationUtils.formatWithChildren(
			"DROP CATALOG FUNCTION",
			params,
			Collections.emptyList(),
			Operation::asSummaryString);
	}

	public boolean isTemporary() {
		return isTemporary;
	}

	public boolean isSystemFunction() {
		return isSystemFunction;
	}

	public String getFunctionName() {
		return this.functionIdentifier.getObjectName();
	}
}
