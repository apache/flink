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

package org.apache.flink.table.runtime.util;

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.runtime.generated.RecordEqualiser;

/**
 * A utility class to check whether two BaseRow are equal.
 * Note: Only support to compare two BinaryRows or two GenericRows.
 */
public class BaseRowRecordEqualiser implements RecordEqualiser {

	@Override
	public boolean equals(BaseRow row1, BaseRow row2) {
		if (row1 instanceof BinaryRow && row2 instanceof BinaryRow) {
			return row1.equals(row2);
		} else if (row1 instanceof GenericRow && row2 instanceof GenericRow) {
			return row1.equals(row2);
		} else {
			throw new UnsupportedOperationException();
		}

	}

	@Override
	public boolean equalsWithoutHeader(BaseRow row1, BaseRow row2) {
		if (row1 instanceof BinaryRow && row2 instanceof BinaryRow) {
			return ((BinaryRow) row1).equalsWithoutHeader(row2);
		} else if (row1 instanceof GenericRow && row2 instanceof GenericRow) {
			return ((GenericRow) row1).equalsWithoutHeader(row2);
		} else {
			throw new UnsupportedOperationException();
		}
	}
}
