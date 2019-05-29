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
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.api.TableSchema;

import java.util.List;

/**
 * A {@link TableOperation} that describes the DML queries such as e.g. INSERT or conversion to a
 * DataStream.
 *
 * <p>A tree of {@link QueryTableOperation} with a {@link ModifyTableOperation} on top
 * represents a runnable query that can be transformed into a graph of
 * {@link org.apache.flink.streaming.api.transformations.StreamTransformation}
 * via {@link org.apache.flink.table.planner.Planner#translate(List, QueryConfig)}
 *
 * <p>It extends from {@link QueryTableOperation} to enable applying subsequent
 * {@link QueryTableOperation}s on top. In other words it makes it possible to have an output
 * transformation as a middle step of a relational query. This is not supported yet.
 *
 * @see QueryTableOperation
 */
@Internal
public abstract class ModifyTableOperation implements QueryTableOperation {

	/**
	 * Currently all DML operations are terminal. They cannot produce output.
	 */
	@Override
	public final TableSchema getTableSchema() {
		throw new UnsupportedOperationException("Currently ModifyTableOperation can only be a terminal operation.");
	}
}
