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

package org.apache.flink.table.type;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.type.InternalTypes.BYTE;
import static org.apache.flink.table.type.InternalTypes.DOUBLE;
import static org.apache.flink.table.type.InternalTypes.FLOAT;
import static org.apache.flink.table.type.InternalTypes.INT;
import static org.apache.flink.table.type.InternalTypes.LONG;
import static org.apache.flink.table.type.InternalTypes.SHORT;

/**
 * Utilities for {@link InternalType}.
 */
public class InternalTypeUtils {

	private static final Map<InternalType, InternalType[]> POSSIBLE_CAST_MAP;

	static {
		Map<InternalType, InternalType[]> autoCastMap = new HashMap<>();
		autoCastMap.put(BYTE, new InternalType[]{SHORT, INT, LONG, FLOAT, DOUBLE});
		autoCastMap.put(SHORT, new InternalType[]{INT, LONG, FLOAT, DOUBLE});
		autoCastMap.put(INT, new InternalType[]{LONG, FLOAT, DOUBLE});
		autoCastMap.put(LONG, new InternalType[]{FLOAT, DOUBLE});
		autoCastMap.put(FLOAT, new InternalType[]{DOUBLE});
		POSSIBLE_CAST_MAP = Collections.unmodifiableMap(autoCastMap);
	}

	/**
	 * Gets the arity of the type.
	 */
	public static int getArity(InternalType t) {
		if (t instanceof RowType) {
			return ((RowType) t).getArity();
		} else {
			return 1;
		}
	}

	public static Class getExternalClassForType(InternalType type) {
		return TypeConverters.createExternalTypeInfoFromInternalType(type).getTypeClass();
	}

	public static Class getInternalClassForType(InternalType type) {
		return TypeConverters.createInternalTypeInfoFromInternalType(type).getTypeClass();
	}

	public static RowType toRowType(InternalType t) {
		if (t instanceof RowType) {
			return (RowType) t;
		} else {
			return new RowType(new InternalType[] {t});
		}
	}

	/**
	 * Returns whether this type should be automatically casted to
	 * the target type in an arithmetic operation.
	 */
	public static boolean shouldAutoCastTo(PrimitiveType from, PrimitiveType to) {
		InternalType[] castTypes = POSSIBLE_CAST_MAP.get(from);
		if (castTypes != null) {
			for (InternalType type : POSSIBLE_CAST_MAP.get(from)) {
				if (type.equals(to)) {
					return true;
				}
			}
		}
		return false;
	}

}
