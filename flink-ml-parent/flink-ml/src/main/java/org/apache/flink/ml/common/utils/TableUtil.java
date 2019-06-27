/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Table Util.
 */
public class TableUtil {
	public static int findIndexFromName(String[] colNames, String name) {
		int idx = -1;
		for (int i = 0; i < colNames.length; i++) {
			if (name.compareToIgnoreCase(colNames[i]) == 0) {
				idx = i;
			}
		}
		return idx;
	}

	public static int[] findIndexFromName(String[] colNames, String[] selectedColNames) {
		if (selectedColNames == null) {
			selectedColNames = colNames;
		}
		int[] idxs = new int[selectedColNames.length];
		for (int i = 0; i < idxs.length; i++) {
			idxs[i] = findIndexFromName(colNames, selectedColNames[i]);
		}
		return idxs;
	}

	public static int[] findIndexFromTable(TableSchema dataSchema, String[] selectedColNames) {
		return findIndexFromName(dataSchema.getFieldNames(), selectedColNames);
	}

	public static TypeInformation[] findTypeFromTable(TableSchema dataSchema, String[] selectedColNames) {
		TypeInformation[] types = new TypeInformation[selectedColNames.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = dataSchema.getFieldType(selectedColNames[i]).get();
		}
		return types;
	}

	public static String getNewTempColName(String[] colNames) {
		String prefix = "mltempcol";
		if (null == colNames) {
			return prefix;
		}
		for (int k = 1; k <= colNames.length + 1; k++) {
			String t = prefix + String.valueOf(k);
			boolean exists = false;
			for (int i = 0; i < colNames.length; i++) {
				if (t.compareToIgnoreCase(colNames[i]) == 0) {
					exists = true;
				}
			}
			if (!exists) {
				return t;
			}
		}
		throw new RuntimeException();
	}

	public static String[] getNumericColNames(TableSchema schema) {
		List <String> excludeColNames = null;
		return getNumericColNames(schema, excludeColNames);
	}

	public static String[] getNumericColNames(TableSchema schema, String[] excludeColNames) {
		return getNumericColNames(schema, (null == excludeColNames) ? null : Arrays.asList(excludeColNames));
	}

	public static String[] getNumericColNames(TableSchema schema, List <String> excludeColNames) {
		HashSet <TypeInformation <?>> hashSet = new HashSet <>();
		hashSet.add(Types.INT);
		hashSet.add(Types.DOUBLE);
		hashSet.add(Types.LONG);
		hashSet.add(Types.FLOAT);
		hashSet.add(Types.SHORT);
		hashSet.add(Types.BYTE);

		ArrayList <String> selectedColNames = new ArrayList <>();
		String[] inColNames = schema.getFieldNames();
		TypeInformation <?>[] inColTypes = schema.getFieldTypes();

		for (int i = 0; i < inColNames.length; i++) {
			if (hashSet.contains(inColTypes[i]) && (null == excludeColNames || !excludeColNames.contains(
				inColNames[i]))) {
				selectedColNames.add(inColNames[i]);
			}
		}

		return selectedColNames.toArray(new String[0]);
	}

	static TypeInformation getColType(TypeInformation[] types, String[] names, String name) {
		int index = getColIndex(names, name);

		if (index == -1) {
			return null;
		}

		return types[index];
	}

	public static int getColIndex(String[] names, String name) {
		int index = -1;
		int len = names.length;

		for (int i = 0; i < len; ++i) {
			if (names[i].equals(name)) {
				index = i;
				break;
			}
		}

		return index;
	}

	public static TypeInformation <?>[] getColTypes(
		TypeInformation <?>[] allTypes, String[] allNames, String[] selected) {
		if (selected == null) {
			return null;
		}

		TypeInformation <?>[] ret = new TypeInformation[selected.length];

		for (int i = 0; i < selected.length; ++i) {
			ret[i] = getColType(allTypes, allNames, selected[i]);
		}

		return ret;
	}

	public static String[] categoricalColNames(
		TableSchema tableSchema, String[] featureColNames, String[] categoricalColNames) {
		if (featureColNames == null) {
			return categoricalColNames;
		}

		TypeInformation[] featureColTypes = findTypeFromTable(tableSchema, featureColNames);

		int[] categoricalInTableIdxs = IntStream.range(0, featureColTypes.length)
			.filter(x -> featureColTypes[x] == Types.STRING || featureColTypes[x] == Types.BOOLEAN)
			.toArray();

		if (categoricalInTableIdxs.length == 0) {
			return categoricalColNames;
		}

		if (categoricalColNames == null) {
			return Arrays.stream(categoricalInTableIdxs)
				.mapToObj(x -> featureColNames[x])
				.toArray(String[]::new);
		}

		return IntStream.concat(
			Arrays.stream(categoricalInTableIdxs),
			Arrays.stream(categoricalColNames)
				.mapToInt(x -> findIndexFromName(featureColNames, x)))
			.distinct()
			.sorted()
			.mapToObj(x -> featureColNames[x])
			.toArray(String[]::new);
	}

}
