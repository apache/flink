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

package org.apache.flink.streaming.connectors.pulsar.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tool class for getting document field types.
 */
@Internal
public class TypeInformationUtils {

	/**
	 * Document Field Type Caching.
	 */
	private static final Map<Class<?>, List<TypeInformation<?>>> typeInfoPerDocument = new ConcurrentHashMap<>();

	private static final Map<Class<?>, TypeInformation<Row>> rowInfoPerDocument = new ConcurrentHashMap<>();

	public static TypeInformation<Row> getTypesAsRow(Class<?> documentFormat) {
		return rowInfoPerDocument.computeIfAbsent(documentFormat, documentFormat1 -> {
			List<TypeInformation<?>> columnTypes = getColumnTypes(documentFormat1);
			TypeInformation<?>[] typeInfos = columnTypes.toArray(new TypeInformation[0]);
			List<String> fieldNames = getFieldNames(documentFormat1);
			return new RowTypeInfo(typeInfos, fieldNames.toArray(new String[0]));
		});
	}

	private static List<String> getFieldNames(Class<?> documentFormat) {
		List<Field> fields = FieldUtils.getAllFieldsList(documentFormat);
		List<String> fieldNames = new ArrayList<>();
		for (Field field : fields) {
			if (Modifier.isStatic(field.getModifiers())) {
				continue;
			}
			fieldNames.add(field.getName());
		}
		return fieldNames;
	}

	public static List<TypeInformation<?>> getColumnTypes(Class<?> documentFormat) {
		return typeInfoPerDocument.computeIfAbsent(documentFormat, documentFormat1 -> {
			List<Field> fields = FieldUtils.getAllFieldsList(documentFormat1);
			List<TypeInformation<?>> typeInfos = new ArrayList<>();
			for (Field field : fields) {
				if (Modifier.isStatic(field.getModifiers())) {
					continue;
				}
				typeInfos.add(TypeInformation.of(field.getType()));
			}
			return typeInfos;
		});
	}
}
