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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DML operation that tells to write to a sink. The sink has to be looked up in a
 * {@link org.apache.flink.table.catalog.Catalog}.
 */
@Internal
public class CatalogSinkModifyOperation implements ModifyOperation {

	private final Map<String, String> staticPartitions;
	private final List<String> tablePath;
	private final QueryOperation child;

	public CatalogSinkModifyOperation(List<String> tablePath, QueryOperation child) {
		this(tablePath, child, new HashMap<>());
	}

	public CatalogSinkModifyOperation(List<String> tablePath,
			QueryOperation child,
			Map<String, String> staticPartitions) {
		this.tablePath = tablePath;
		this.child = child;
		this.staticPartitions = staticPartitions;
	}

	public List<String> getTablePath() {
		return tablePath;
	}

	public Map<String, String> getStaticPartitions() {
		return staticPartitions;
	}

	@Override
	public QueryOperation getChild() {
		return child;
	}

	@Override
	public <T> T accept(ModifyOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> params = new LinkedHashMap<>();
		params.put("tablePath", tablePath);
		params.put("staticPartitions", staticPartitions);

		return OperationUtils.formatWithChildren(
			"CatalogSink",
			params,
			Collections.singletonList(child),
			Operation::asSummaryString);
	}
}
