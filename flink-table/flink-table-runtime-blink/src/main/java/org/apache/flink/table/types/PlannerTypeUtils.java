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

package org.apache.flink.table.types;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/**
 * Utilities for {@link LogicalType} and {@link DataType}..
 */
public class PlannerTypeUtils {

	/**
	 * Gets the arity of the type.
	 */
	public static int getArity(LogicalType t) {
		if (t instanceof RowType) {
			return ((RowType) t).getFieldCount();
		} else {
			return 1;
		}
	}

	public static RowType toRowType(LogicalType t) {
		if (t instanceof RowType) {
			return (RowType) t;
		} else {
			return RowType.of(t);
		}
	}

	public static boolean isPrimitive(LogicalType type) {
		return isPrimitive(type.getTypeRoot());
	}

	public static boolean isPrimitive(LogicalTypeRoot root) {
		switch (root) {
			case BOOLEAN:
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case FLOAT:
			case DOUBLE:
				return true;
			default:
				return false;
		}
	}

	/**
	 * Can the two types operate with each other.
	 * Such as:
	 * 1.CodeGen: equal, cast, assignment.
	 * 2.Join keys.
	 */
	public static boolean isInteroperable(LogicalType t1, LogicalType t2) {
		if (t1.getTypeRoot() != t2.getTypeRoot()) {
			return false;
		}

		switch (t1.getTypeRoot()) {
			// VARCHAR VARBINARY ignore length.
			case VARCHAR:
			case VARBINARY:
				return true;
			case ARRAY:
			case MAP:
			case MULTISET:
			case ROW:
				List<LogicalType> children1 = t1.getChildren();
				List<LogicalType> children2 = t2.getChildren();
				if (children1.size() != children2.size()) {
					return false;
				}
				for (int i = 0; i < children1.size(); i++) {
					if (!isInteroperable(children1.get(i), children2.get(i))) {
						return false;
					}
				}
				return true;
			default:
				return t1.copy(true).equals(t2.copy(true));
		}
	}
}
