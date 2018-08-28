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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Collects results and returns them as table snapshots.
 *
 * @param <C> cluster id to which this result belongs to
 */
public class MaterializedCollectStreamResult<C> extends CollectStreamResult<C> implements MaterializedResult<C> {

	private final List<Row> materializedTable;

	/**
	 * Caches the last row position for faster access. The position might not be exact (if rows
	 * with smaller position are deleted) nor complete (for deletes of duplicates). However, the
	 * cache narrows the search in the materialized table.
	 */
	private final Map<Row, Integer> rowPositionCache;

	private final List<Row> snapshot;

	private int pageCount;

	private int pageSize;

	private boolean isLastSnapshot;

	public MaterializedCollectStreamResult(TypeInformation<Row> outputType, ExecutionConfig config,
			InetAddress gatewayAddress, int gatewayPort) {
		super(outputType, config, gatewayAddress, gatewayPort);

		// prepare for materialization
		materializedTable = new ArrayList<>();
		rowPositionCache = new HashMap<>();
		snapshot = new ArrayList<>();
		isLastSnapshot = false;
		pageCount = 0;
	}

	@Override
	public boolean isMaterialized() {
		return true;
	}

	@Override
	public TypedResult<Integer> snapshot(int pageSize) {
		synchronized (resultLock) {
			// retrieval thread is dead and there are no results anymore
			// or program failed
			if ((!isRetrieving() && isLastSnapshot) || executionException != null) {
				return handleMissingResult();
			}
			// this snapshot is the last result that can be delivered
			else if (!isRetrieving()) {
				isLastSnapshot = true;
			}

			this.pageSize = pageSize;
			snapshot.clear();
			snapshot.addAll(materializedTable);

			// at least one page
			pageCount = Math.max(1, (int) Math.ceil(((double) snapshot.size() / pageSize)));

			return TypedResult.payload(pageCount);
		}
	}

	@Override
	public List<Row> retrievePage(int page) {
		synchronized (resultLock) {
			if (page <= 0 || page > pageCount) {
				throw new SqlExecutionException("Invalid page '" + page + "'.");
			}

			return snapshot.subList(pageSize * (page - 1), Math.min(snapshot.size(), pageSize * page));
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	protected void processRecord(Tuple2<Boolean, Row> change) {
		synchronized (resultLock) {
			final Row row = change.f1;
			// insert
			if (change.f0) {
				materializedTable.add(row);
				rowPositionCache.put(row, materializedTable.size() - 1);
			}
			// delete
			else {
				// delete the newest record first to minimize per-page changes
				final Integer cachedPos = rowPositionCache.get(row);
				final int startSearchPos;
				if (cachedPos != null) {
					startSearchPos = Math.min(cachedPos, materializedTable.size() - 1);
				} else {
					startSearchPos = materializedTable.size() - 1;
				}

				for (int i = startSearchPos; i >= 0; i--) {
					if (materializedTable.get(i).equals(row)) {
						materializedTable.remove(i);
						rowPositionCache.remove(row);
						break;
					}
				}
			}
		}
	}
}
