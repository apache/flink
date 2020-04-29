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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.RecordEqualiser;

/**
 * A utility class to check whether two RowData are equal.
 * Note: Only support to compare two BinaryRows or two GenericRows.
 */
public class RowDataRecordEqualiser implements RecordEqualiser {

	private static final long serialVersionUID = -6706336100425614942L;

	@Override
	public boolean equals(RowData row1, RowData row2) {
		if (row1 instanceof BinaryRowData && row2 instanceof BinaryRowData) {
			return row1.equals(row2);
		} else if (row1 instanceof GenericRowData && row2 instanceof GenericRowData) {
			return row1.equals(row2);
		} else {
			throw new UnsupportedOperationException();
		}

	}
}
