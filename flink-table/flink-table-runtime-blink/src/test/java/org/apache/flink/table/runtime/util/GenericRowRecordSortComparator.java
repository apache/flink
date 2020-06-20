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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.util.Comparator;

/**
 * A utility class to compare two GenericRowData based on sortKey value.
 * Note: Only support sortKey is Comparable value.
 */
public class GenericRowRecordSortComparator implements Comparator<GenericRowData>, Serializable {

	private static final long serialVersionUID = -4988371592272863772L;

	private final int sortKeyIdx;
	private final LogicalType sortKeyType;

	public GenericRowRecordSortComparator(int sortKeyIdx, LogicalType sortKeyType) {
		this.sortKeyIdx = sortKeyIdx;
		this.sortKeyType = sortKeyType;
	}

	@Override
	public int compare(GenericRowData row1, GenericRowData row2) {
		RowKind kind1 = row1.getRowKind();
		RowKind kind2 = row2.getRowKind();
		if (kind1 != kind2) {
			return kind1.toByteValue() - kind2.toByteValue();
		} else {
			Object key1 = RowData.get(row1, sortKeyIdx, sortKeyType);
			Object key2 = RowData.get(row2, sortKeyIdx, sortKeyType);
			if (key1 instanceof Comparable) {
				return ((Comparable) key1).compareTo(key2);
			} else {
				throw new UnsupportedOperationException();
			}
		}
	}
}
