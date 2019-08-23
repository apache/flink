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

package org.apache.flink.table.runtime.types;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

import static org.apache.flink.table.types.logical.LogicalTypeFamily.BINARY_STRING;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.CHARACTER_STRING;

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
		if (t1.getTypeRoot().getFamilies().contains(CHARACTER_STRING) &&
				t2.getTypeRoot().getFamilies().contains(CHARACTER_STRING)) {
			return true;
		}
		if (t1.getTypeRoot().getFamilies().contains(BINARY_STRING) &&
				t2.getTypeRoot().getFamilies().contains(BINARY_STRING)) {
			return true;
		}
		if (t1.getTypeRoot() != t2.getTypeRoot()) {
			return false;
		}

		switch (t1.getTypeRoot()) {
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

	/**
	 * Now in the conversion to the TypeInformation from DataType, type may loose some information
	 * about nullable and precision. So we add this method to do a soft check.
	 *
	 * <p>The difference of {@link #isInteroperable} is ignore decimal precision.
	 *
	 * <p>Now not ignore timestamp precision, because we only support one precision for timestamp type now.
	 */
	public static boolean isAssignable(LogicalType t1, LogicalType t2) {
		// Soft check for CharType, it is converted to String TypeInformation and loose char information.
		if (t1.getTypeRoot().getFamilies().contains(CHARACTER_STRING) &&
				t2.getTypeRoot().getFamilies().contains(CHARACTER_STRING)) {
			return true;
		}
		if (t1.getTypeRoot().getFamilies().contains(BINARY_STRING) &&
				t2.getTypeRoot().getFamilies().contains(BINARY_STRING)) {
			return true;
		}
		if (t1.getTypeRoot() != t2.getTypeRoot()) {
			return false;
		}

		switch (t1.getTypeRoot()) {
			case DECIMAL:
				return true;
			default:
				if (t1.getChildren().isEmpty()) {
					return t1.copy(true).equals(t2.copy(true));
				} else {
					List<LogicalType> children1 = t1.getChildren();
					List<LogicalType> children2 = t2.getChildren();
					if (children1.size() != children2.size()) {
						return false;
					}
					for (int i = 0; i < children1.size(); i++) {
						if (!isAssignable(children1.get(i), children2.get(i))) {
							return false;
						}
					}
					return true;
				}
		}
	}
}
