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

import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.OperationUtils;

import java.util.stream.Collectors;

/**
 * Operation to describe a ALTER VIEW .. SET .. statement.
 */
public class AlterViewPropertiesOperation extends AlterViewOperation {

	private final CatalogView catalogView;

	public AlterViewPropertiesOperation(ObjectIdentifier viewIdentifier, CatalogView catalogView) {
		super(viewIdentifier);
		this.catalogView = catalogView;
	}

	public CatalogView getCatalogView() {
		return catalogView;
	}

	@Override
	public String asSummaryString() {
		String description = catalogView.getOptions().entrySet().stream()
				.map(entry -> OperationUtils.formatParameter(entry.getKey(), entry.getValue()))
				.collect(Collectors.joining(", "));
		return String.format("ALTER VIEW %s SET (%s)", viewIdentifier.asSummaryString(), description);
	}
}
