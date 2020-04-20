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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.ModifyOperation;

import java.util.List;

/**
 * An internal interface of {@link TableEnvironment}
 * that defines extended methods used for {@link TableImpl}.
 */
@Internal
public interface TableEnvironmentInternal extends TableEnvironment {

	/**
	 * Return a {@link Parser} that provides methods for parsing a SQL string.
	 *
	 * @return initialized {@link Parser}.
	 */
	Parser getParser();

	/**
	 * Returns a {@link CatalogManager} that deals with all catalog objects.
	 */
	CatalogManager getCatalogManager();

	/**
	 * Execute the given operations and return the execution result.
	 *
	 * @param operations The operations to be executed.
	 * @return the affected row counts (-1 means unknown).
	 */
	TableResult executeOperations(List<ModifyOperation> operations);
}
