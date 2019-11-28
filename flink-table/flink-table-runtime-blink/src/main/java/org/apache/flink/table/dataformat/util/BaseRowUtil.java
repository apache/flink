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

package org.apache.flink.table.dataformat.util;

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Util for base row.
 */
public final class BaseRowUtil {

	/**
	 * Indicates the row as an accumulate message.
	 */
	public static final byte ACCUMULATE_MSG = 0;

	/**
	 * Indicates the row as a retraction message.
	 */
	public static final byte RETRACT_MSG = 1;

	public static boolean isAccumulateMsg(BaseRow baseRow) {
		return baseRow.getHeader() == ACCUMULATE_MSG;
	}

	public static boolean isRetractMsg(BaseRow baseRow) {
		return baseRow.getHeader() == RETRACT_MSG;
	}

	public static BaseRow setAccumulate(BaseRow baseRow) {
		baseRow.setHeader(ACCUMULATE_MSG);
		return baseRow;
	}

	public static BaseRow setRetract(BaseRow baseRow) {
		baseRow.setHeader(RETRACT_MSG);
		return baseRow;
	}

	public static GenericRow toGenericRow(
			BaseRow baseRow,
			LogicalType[] types) {
		if (baseRow instanceof GenericRow) {
			return (GenericRow) baseRow;
		} else {
			GenericRow row = new GenericRow(baseRow.getArity());
			row.setHeader(baseRow.getHeader());
			for (int i = 0; i < row.getArity(); i++) {
				if (baseRow.isNullAt(i)) {
					row.setField(i, null);
				} else {
					row.setField(i, TypeGetterSetters.get(baseRow, i, types[i]));
				}
			}
			return row;
		}
	}
}
