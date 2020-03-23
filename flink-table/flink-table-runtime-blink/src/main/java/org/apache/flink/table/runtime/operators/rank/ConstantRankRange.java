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

/** rankStart and rankEnd are inclusive, rankStart always start from one. */
public class ConstantRankRange implements RankRange {

	private static final long serialVersionUID = 9062345289888078376L;
	private long rankStart;
	private long rankEnd;

	public ConstantRankRange(long rankStart, long rankEnd) {
		this.rankStart = rankStart;
		this.rankEnd = rankEnd;
	}

	public long getRankStart() {
		return rankStart;
	}

	public long getRankEnd() {
		return rankEnd;
	}

	@Override
	public String toString(List<String> inputFieldNames) {
		return toString();
	}

	@Override
	public String toString() {
		return "rankStart=" + rankStart + ", rankEnd=" + rankEnd;
	}

}
