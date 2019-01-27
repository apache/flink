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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.dataformat.BaseRow;

import java.util.Map;
import java.util.Set;

/**
 * The implementation for {@link JoinMatchStateHandler}.
 */
@Internal
public class NonBatchOnlyEqualityConditionMatchStateHandler implements JoinMatchStateHandler {

	private long matchJoinCnt;

	public NonBatchOnlyEqualityConditionMatchStateHandler() {
	}

	@Override
	public void extractCurrentRowMatchJoinCount(BaseRow joinKey, BaseRow row, long possibleJoinCnt) {
		matchJoinCnt = possibleJoinCnt;
	}

	@Override
	public long getCurrentRowMatchJoinCnt() {
		return matchJoinCnt;
	}

	@Override
	public void resetCurrentRowMatchJoinCnt(long joinCnt) {
		matchJoinCnt = joinCnt;
	}

	@Override
	public void updateRowMatchJoinCnt(BaseRow joinKey, BaseRow baseRow, long joinCnt) {
		matchJoinCnt = joinCnt;
	}

	@Override
	public void addRowMatchJoinCnt(BaseRow joinKey, BaseRow baseRow, long joinCnt) {
		matchJoinCnt += joinCnt;
	}

	@Override
	public void remove(BaseRow joinKey, BaseRow baseRow) {

	}

	@Override
	public void remove(BaseRow joinKey) {

	}

	@Override
	public void removeAll(BaseRow joinKey, Set<BaseRow> keys) {

	}

	@Override
	public void addAll(
			BaseRow joinKey, Map<BaseRow, Long> kvs) {

	}
}
