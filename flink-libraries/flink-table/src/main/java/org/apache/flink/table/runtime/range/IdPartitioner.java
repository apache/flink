/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.range;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.util.Preconditions;

/**
 * Partition according to the key.
 */
@Internal
public class IdPartitioner implements Partitioner<Integer> {

	private static final long serialVersionUID = -1206233785103357568L;
	private final int totalRangeNum;

	public IdPartitioner(int totalRangeNum) {
		this.totalRangeNum = totalRangeNum;
	}

	@Override
	public int partition(Integer key, int numPartitions) {
		Preconditions.checkArgument(numPartitions < totalRangeNum, "Num of subPartitions should < totalRangeNum: " + totalRangeNum);
		double partitionSize = totalRangeNum / numPartitions;
		int partition = (int) (key / partitionSize);
		// partition => [0, numPartitions -1], but may >= numPartitions-1 as double.
		return partition >= numPartitions ? numPartitions - 1 : partition;
	}
}
