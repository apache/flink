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

package org.apache.flink.table.sources.orc;

import org.apache.orc.Reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utils for Orc connector.
 */
public class OrcUtils {
	public static int[] requestedColumnIds(
			boolean isCaseSensitive,
			String[] requiredFieldNames,
			String[] schemaFieldNames,
			Reader reader) {
		String[] orcFieldNames = reader.getSchema().getFieldNames().toArray(new String[0]);
		if (orcFieldNames.length == 0) {
			// Some old empty ORC files always have an empty schema stored in their footer.
			return null;
		} else {
			boolean isHiveOrcFile = Arrays.stream(orcFieldNames).allMatch(field -> field.startsWith("_col"));
			List<Integer> ret = new ArrayList<>();
			if (isHiveOrcFile) {
				// This is a ORC file written by Hive.
				// assume the required field names are the physical field names.
				for (String field : requiredFieldNames) {
					int index = fieldIndex(field, schemaFieldNames, isCaseSensitive);
					ret.add(index);
				}
			} else {
				for (String field : requiredFieldNames) {
					int index = fieldIndex(field, orcFieldNames, isCaseSensitive);
					ret.add(index);
				}
			}

			return ret.stream().mapToInt(i -> i).toArray();
		}
	}

	// TODO: performance issue?
	private static int fieldIndex(String field, String[] fieldNames, boolean isCaseSensitive) {
		for (int i = 0; i < fieldNames.length; i++) {
			if ((isCaseSensitive && field.equals(fieldNames[i]))
				|| (!isCaseSensitive && field.equalsIgnoreCase(fieldNames[i]))) {
				return i;
			}
		}
		return -1;
	}
}
