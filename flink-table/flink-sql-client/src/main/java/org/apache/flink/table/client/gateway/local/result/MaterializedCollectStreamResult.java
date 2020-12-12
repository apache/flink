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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

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

	/** Maximum initial capacity of the materialized table. */
	public static final int MATERIALIZED_TABLE_MAX_INITIAL_CAPACITY = 1_000_000;

	/** Maximum overcommitment of the materialized table. */
	public static final int MATERIALIZED_TABLE_MAX_OVERCOMMIT = 1_000_000;

	/** Factor for the initial capacity of the materialized table. */
	public static final double MATERIALIZED_TABLE_CAPACITY_FACTOR = 0.05;

	/** Factor for cleaning up deleted rows in the materialized table. */
	public static final double MATERIALIZED_TABLE_OVERCOMMIT_FACTOR = 0.01;

	/**
	 * Maximum number of materialized rows to be stored. After the count is reached, oldest
	 * rows are dropped.
	 */
	private final int maxRowCount;

	/** Threshold for cleaning up deleted rows in the materialized table. */
	private final int overcommitThreshold;

	/**
	 * Materialized table that is continuously updated by inserts and deletes. Deletes at
	 * the beginning are lazily cleaned up when the threshold is reached.
	 */
	private final List<Row> materializedTable;

	/**
	 * Caches the last row position for faster access. The position might not be exact (if rows
	 * with smaller position are deleted) nor complete (for deletes of duplicates). However, the
	 * cache narrows the search in the materialized table.
	 */
	private final Map<Row, Integer> rowPositionCache;

	/** Current snapshot of the materialized table. */
	private final List<Row> snapshot;

	/** Counter for deleted rows to be deleted at the beginning of the materialized table. */
	private int validRowPosition;

	/** Page count of the snapshot (always >= 1). */
	private int pageCount;

	/** Page size of the snapshot (always >= 1). */
	private int pageSize;

	/** Indicator that this is the last snapshot possible (EOS afterwards). */
	private boolean isLastSnapshot;

	@VisibleForTesting
	public MaterializedCollectStreamResult(
			TableSchema tableSchema,
			ExecutionConfig config,
			InetAddress gatewayAddress,
			int gatewayPort,
			int maxRowCount,
			int overcommitThreshold) {
		super(tableSchema, config, gatewayAddress, gatewayPort);

		if (maxRowCount <= 0) {
			this.maxRowCount = Integer.MAX_VALUE;
		} else {
			this.maxRowCount = maxRowCount;
		}

		this.overcommitThreshold = overcommitThreshold;

		// prepare for materialization
		final int initialCapacity = computeMaterializedTableCapacity(maxRowCount); // avoid frequent resizing
		materializedTable = new ArrayList<>(initialCapacity);
		rowPositionCache = new HashMap<>(initialCapacity);
		snapshot = new ArrayList<>();
		validRowPosition = 0;
		isLastSnapshot = false;
		pageCount = 0;
	}

	public MaterializedCollectStreamResult(
			TableSchema tableSchema,
			ExecutionConfig config,
			InetAddress gatewayAddress,
			int gatewayPort,
			int maxRowCount) {

		this(
			tableSchema,
			config,
			gatewayAddress,
			gatewayPort,
			maxRowCount,
			computeMaterializedTableOvercommit(maxRowCount));
	}

	@Override
	public boolean isMaterialized() {
		return true;
	}

	@Override
	public TypedResult<Integer> snapshot(int pageSize) {
		if (pageSize < 1) {
			throw new SqlExecutionException("Page size must be greater than 0.");
		}

		synchronized (resultLock) {
			// retrieval thread is dead and there are no results anymore
			// or program failed
			if ((!isRetrieving() && isLastSnapshot) || executionException.get() != null) {
				return handleMissingResult();
			}
			// this snapshot is the last result that can be delivered
			else if (!isRetrieving()) {
				isLastSnapshot = true;
			}

			this.pageSize = pageSize;
			snapshot.clear();
			for (int i = validRowPosition; i < materializedTable.size(); i++) {
				snapshot.add(materializedTable.get(i));
			}

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
			// Always set the RowKind to INSERT, so that we can compare rows correctly (RowKind will be ignored),
			// just use the Boolean of Tuple2<Boolean, Row> to figure out whether it is insert or delete.
			change.f1.setKind(RowKind.INSERT);

			// insert
			if (change.f0) {
				processInsert(change.f1);
			}
			// delete
			else {
				processDelete(change.f1);
			}
		}
	}

	@VisibleForTesting
	protected List<Row> getMaterializedTable() {
		return materializedTable;
	}

	// --------------------------------------------------------------------------------------------

	private void processInsert(Row row) {
		// limit the materialized table
		if (materializedTable.size() - validRowPosition >= maxRowCount) {
			cleanUp();
		}
		materializedTable.add(row);
		rowPositionCache.put(row, materializedTable.size() - 1);
	}

	private void processDelete(Row row) {
		// delete the newest record first to minimize per-page changes
		final Integer cachedPos = rowPositionCache.get(row);
		final int startSearchPos;
		if (cachedPos != null) {
			startSearchPos = Math.min(cachedPos, materializedTable.size() - 1);
		} else {
			startSearchPos = materializedTable.size() - 1;
		}

		for (int i = startSearchPos; i >= validRowPosition; i--) {
			if (materializedTable.get(i).equals(row)) {
				materializedTable.remove(i);
				rowPositionCache.remove(row);
				break;
			}
		}
	}

	private void cleanUp() {
		// invalidate row
		final Row deleteRow = materializedTable.get(validRowPosition);
		if (rowPositionCache.get(deleteRow) == validRowPosition) {
			// this row has no duplicates in the materialized table,
			// it can be removed from the cache
			rowPositionCache.remove(deleteRow);
		}
		materializedTable.set(validRowPosition, null);

		validRowPosition++;

		// perform clean up in batches
		if (validRowPosition >= overcommitThreshold) {
			materializedTable.subList(0, validRowPosition).clear();
			// adjust all cached indexes
			rowPositionCache.replaceAll((k, v) -> v - validRowPosition);
			validRowPosition = 0;
		}
	}

	private static int computeMaterializedTableCapacity(int maxRowCount) {
		return Math.min(
			MATERIALIZED_TABLE_MAX_INITIAL_CAPACITY,
			Math.max(1, (int) (maxRowCount * MATERIALIZED_TABLE_CAPACITY_FACTOR)));
	}

	private static int computeMaterializedTableOvercommit(int maxRowCount) {
		return Math.min(
			MATERIALIZED_TABLE_MAX_OVERCOMMIT,
			(int) (maxRowCount * MATERIALIZED_TABLE_OVERCOMMIT_FACTOR));
	}
}
