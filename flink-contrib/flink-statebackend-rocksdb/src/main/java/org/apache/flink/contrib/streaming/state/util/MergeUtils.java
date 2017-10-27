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

package org.apache.flink.contrib.streaming.state.util;

import org.apache.flink.annotation.VisibleForTesting;

import java.util.List;

/**
 * Utils to simulate StringAppendTestOperator's merge operations in RocksDB.
 */
public class MergeUtils {
	@VisibleForTesting
	protected static final byte DELIMITER = ',';

	/**
	 * Merge operands into a single value that can be put directly into RocksDB.
	 */
	public static byte[] merge(List<byte[]> operands) {
		if (operands == null || operands.size() == 0) {
			return null;
		}

		if (operands.size() == 1) {
			return operands.get(0);
		}

		int numBytes = 0;
		for (byte[] arr : operands) {
			numBytes += arr.length + 1;
		}
		numBytes--;

		byte[] result = new byte[numBytes];

		System.arraycopy(operands.get(0), 0, result, 0, operands.get(0).length);

		for (int i = 1, arrIndex = operands.get(0).length; i < operands.size(); i++) {
			result[arrIndex] = DELIMITER;
			arrIndex += 1;
			System.arraycopy(operands.get(i), 0, result, arrIndex, operands.get(i).length);
			arrIndex += operands.get(i).length;
		}

		return result;
	}
}
