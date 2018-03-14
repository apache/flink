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

package org.apache.flink.table.client.gateway.local;

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
	private final Map<Row, List<Integer>> rowPositions; // positions of rows in table for faster access
	private final List<Row> snapshot;
	private int pageCount;
	private int pageSize;
	private boolean isLastSnapshot;

	public MaterializedCollectStreamResult(TypeInformation<Row> outputType, ExecutionConfig config,
			InetAddress gatewayAddress, int gatewayPort) {
		super(outputType, config, gatewayAddress, gatewayPort);

		// prepare for materialization
		materializedTable = new ArrayList<>();
		rowPositions = new HashMap<>();
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
		// we track the position of rows for faster access and in order to return consistent
		// snapshots where new rows are appended at the end
		synchronized (resultLock) {
			final List<Integer> positions = rowPositions.get(change.f1);

			// insert
			if (change.f0) {
				materializedTable.add(change.f1);
				if (positions == null) {
					// new row
					final ArrayList<Integer> pos = new ArrayList<>(1);
					pos.add(materializedTable.size() - 1);
					rowPositions.put(change.f1, pos);
				} else {
					// row exists already, only add position
					positions.add(materializedTable.size() - 1);
				}
			}
			// delete
			else {
				if (positions != null) {
					// delete row position and row itself
					final int pos = positions.remove(positions.size() - 1);
					materializedTable.remove(pos);
					if (positions.isEmpty()) {
						rowPositions.remove(change.f1);
					}
				}
			}
		}
	}
}
