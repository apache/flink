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

import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.table.dataformat.BaseRow;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The implementation for {@link JoinMatchStateHandler}.
 */
public class JoinKeyContainPrimaryKeyMatchStateHandler implements JoinMatchStateHandler {

	private final KeyedValueState<BaseRow, Long> keyedValueState;

	private transient BaseRow currentJoinKey;

	private transient long currentRowMatchJoinCount;

	public JoinKeyContainPrimaryKeyMatchStateHandler(KeyedValueState<BaseRow, Long> keyedValueState) {
		this.keyedValueState = keyedValueState;
	}

	@Override
	public void extractCurrentRowMatchJoinCount(BaseRow joinKey, BaseRow row, long possibleJoinCnt) {
		this.currentJoinKey = joinKey;
		Long count = keyedValueState.get(joinKey);
		if (null == count) {
			this.currentRowMatchJoinCount = 0;
		} else {
			this.currentRowMatchJoinCount = count;
		}
	}

	@Override
	public long getCurrentRowMatchJoinCnt() {
		return currentRowMatchJoinCount;
	}

	@Override
	public void resetCurrentRowMatchJoinCnt(long joinCnt) {
		keyedValueState.put(currentJoinKey, joinCnt);
		this.currentRowMatchJoinCount = joinCnt;
	}

	@Override
	public void updateRowMatchJoinCnt(BaseRow joinKey, BaseRow baseRow, long joinCnt) {
		keyedValueState.put(joinKey, joinCnt);
	}

	@Override
	public void addRowMatchJoinCnt(BaseRow joinKey, BaseRow baseRow, long joinCnt) {
		keyedValueState.put(joinKey, joinCnt);
	}

	@Override
	public void remove(BaseRow joinKey, BaseRow baseRow) {
		keyedValueState.remove(joinKey);
	}

	@Override
	public void remove(BaseRow joinKey) {
		keyedValueState.remove(joinKey);
	}

	@Override
	public void removeAll(BaseRow joinKey, Set<BaseRow> keys) {
		if (!keys.isEmpty()) {
			Set<BaseRow> set = new HashSet<>();
			set.add(joinKey);
			keyedValueState.removeAll(set);
		}
	}

	@Override
	public void addAll(BaseRow joinKey, Map<BaseRow, Long> kvs) {
		if (!kvs.isEmpty()) {
			Map<BaseRow, Long> putMap = new HashMap<>();
			for (Map.Entry<BaseRow, Long> entry: kvs.entrySet()) {
				putMap.put(joinKey, entry.getValue());
			}
			keyedValueState.putAll(putMap);
		}
	}
}
