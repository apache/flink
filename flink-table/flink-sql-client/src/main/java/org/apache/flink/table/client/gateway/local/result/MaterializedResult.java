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

package org.apache.flink.table.client.gateway.local.result;

import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * A result that is materialized and can be viewed by navigating through a snapshot.
 *
 * @param <C> cluster id to which this result belongs to
 */
public interface MaterializedResult<C> extends DynamicResult<C> {

	/**
	 * Takes a snapshot of the current table and returns the number of pages for navigating
	 * through the snapshot.
	 */
	TypedResult<Integer> snapshot(int pageSize);

	/**
	 * Retrieves a page of a snapshotted result.
	 */
	List<Row> retrievePage(int page);
}
