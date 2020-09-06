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

package org.apache.flink.table.runtime.operators.rank;

import java.util.List;

/**
 * changing rank limit depends on input.
 */
public class VariableRankRange implements RankRange {

	private static final long serialVersionUID = 5579785886506433955L;
	private int rankEndIndex;

	public VariableRankRange(int rankEndIndex) {
		this.rankEndIndex = rankEndIndex;
	}

	public int getRankEndIndex() {
		return rankEndIndex;
	}

	@Override
	public String toString(List<String> inputFieldNames) {
		return "rankEnd=" + inputFieldNames.get(rankEndIndex);
	}

	@Override
	public String toString() {
		return "rankEnd=$" + rankEndIndex;
	}

}
