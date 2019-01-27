/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copysecond ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.join.stream.state.match;

import org.apache.flink.runtime.state.keyed.KeyedMapState;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.util.StateUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * The implementation for {@link JoinMatchStateHandler}.
 */
public class WithoutPrimaryKeyMatchStateHandler implements JoinMatchStateHandler {

	private static final Logger LOG = LoggerFactory.getLogger(WithoutPrimaryKeyMatchStateHandler.class);
	private final KeyedMapState<BaseRow, BaseRow, Long> keyedMapState;

	private transient BaseRow currentJoinKey;

	private transient BaseRow currentRow;

	private transient long currentRowMatchJoinCont;

	public WithoutPrimaryKeyMatchStateHandler(KeyedMapState<BaseRow, BaseRow, Long> keyedMapState) {
		this.keyedMapState = keyedMapState;
	}

	@Override
	public void extractCurrentRowMatchJoinCount(BaseRow joinKey, BaseRow row, long possibleJoinCnt) {
		this.currentJoinKey = joinKey;
		this.currentRow = row;
		Long count = keyedMapState.get(joinKey, row);
		if (null == count) {
			this.currentRowMatchJoinCont = 0;
		} else {
			this.currentRowMatchJoinCont = count;
		}
	}

	@Override
	public long getCurrentRowMatchJoinCnt() {
		return currentRowMatchJoinCont;
	}

	@Override
	public void resetCurrentRowMatchJoinCnt(long joinCnt) {
		keyedMapState.add(currentJoinKey, currentRow, joinCnt);
		this.currentRowMatchJoinCont = joinCnt;
	}

	@Override
	public void updateRowMatchJoinCnt(BaseRow joinKey, BaseRow baseRow, long joinCnt) {
		keyedMapState.add(joinKey, baseRow, joinCnt);
	}

	@Override
	public void addRowMatchJoinCnt(BaseRow joinKey, BaseRow baseRow, long joinCnt) {
		Long count = keyedMapState.get(joinKey, baseRow);
		if (count != null) {
			keyedMapState.add(joinKey, baseRow, joinCnt + count);
		} else {
			// Assume the history count is 0 if state value is cleared.
			LOG.warn(StateUtil.STATE_CLEARED_WARN_MSG);
			keyedMapState.add(joinKey, baseRow, joinCnt);
		}
	}

	@Override
	public void remove(BaseRow joinKey, BaseRow baseRow) {
		keyedMapState.remove(joinKey, baseRow);
	}

	@Override
	public void remove(BaseRow joinKey) {
		keyedMapState.remove(joinKey);
	}

	@Override
	public void removeAll(BaseRow joinKey, Set<BaseRow> keys) {
		keyedMapState.removeAll(joinKey, keys);
	}

	@Override
	public void addAll(BaseRow joinKey, Map<BaseRow, Long> kvs) {
		keyedMapState.addAll(joinKey, kvs);
	}
}
