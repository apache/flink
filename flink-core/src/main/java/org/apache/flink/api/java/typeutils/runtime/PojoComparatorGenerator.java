/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.CompositeTypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.PojoTypeInfo.accessStringForField;

public final class PojoComparatorGenerator<T> {
	private static final String packageName = "org.apache.flink.api.java.typeutils.runtime.generated";

	private transient Field[] keyFields;
	private transient Integer[] keyFieldIds;
	private final TypeComparator<?>[] comparators;
	private final TypeSerializer<T> serializer;
	private final Class<T> type;
	private final ExecutionConfig config;
	private String code;

	public PojoComparatorGenerator(Field[] keyFields, TypeComparator<?>[] comparators, TypeSerializer<T> serializer,
									Class<T> type, Integer[] keyFieldIds, ExecutionConfig config) {
		this.keyFields = keyFields;
		this.comparators = comparators;

		this.type = type;
		this.serializer = serializer;
		this.keyFieldIds = keyFieldIds;
		this.config = config;
	}

	public TypeComparator<T> createComparator() {
		// Multiple comparators can be generated for each type based on a list of keys. The list of keys and the type
		// name should determine the generated comparator. This information is used for caching (avoiding
		// recompilation). Note that, the name of the field is not sufficient because nested POJOs might have a field
		// with the name.
		StringBuilder keyBuilder = new StringBuilder();
		for(Integer i : keyFieldIds) {
			keyBuilder.append(i);
			keyBuilder.append("_");
		}
		final String className = type.getCanonicalName().replace('.', '_') + "_GeneratedComparator" +
			keyBuilder.toString();
		final String fullClassName = packageName + "." + className;
		code = InstantiationUtil.getCodeForCachedClass(fullClassName);
		if (code == null) {
			generateCode(className);
		}
		return new GenTypeComparatorProxy<>(type, fullClassName, code, comparators, serializer);
	}


	private void generateCode(String className) {
		String typeName = type.getCanonicalName();
		StringBuilder members = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			String comparatorClass = comparators[i].getClass().getCanonicalName();
			members.append(String.format("final %s f%d;\n", comparatorClass, i));
		}
		StringBuilder initMembers = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			String comparatorClass = comparators[i].getClass().getCanonicalName();
			initMembers.append(String.format("f%d = (%s)comparators[%d];\n", i, comparatorClass, i));
		}
		StringBuilder normalizableKeys = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			normalizableKeys.append(String.format("if (f%d.supportsNormalizedKey()) {\n" +
				"	if (f%d.invertNormalizedKey() != inverted) break;\n" +
				"	nKeys++;\n" +
				"	final int len = f%d.getNormalizeKeyLen();\n" +
				"	this.normalizedKeyLengths[%d] = len;\n" +
				"	nKeyLen += len;\n" +
				"	if (nKeyLen < 0) {\n" +
				"		nKeyLen = Integer.MAX_VALUE;\n" +
				"		break;\n" +
				"	}\n" +
				"} else {\n" +
				"	break;\n" +
				"}\n", i, i, i, i));
		}
		StringBuilder cloneMembers = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			String comparatorClass = comparators[i].getClass().getCanonicalName();
			cloneMembers.append(String.format("f%d = (%s)toClone.f%d.duplicate();\n", i, comparatorClass, i));
		}
		StringBuilder flatComparators = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			if (comparators[i] instanceof CompositeTypeComparator) {
				flatComparators.append(String.format("((CompositeTypeComparator)f%d).getFlatComparator" +
					"(flatComparators);\n", i));
			} else {
				flatComparators.append(String.format("flatComparators.add(f%d);\n", i));
			}
		}
		StringBuilder hashMembers = new StringBuilder();
		for (int i = 0; i < keyFields.length; ++i) {
			hashMembers.append(String.format(
				"code *= TupleComparatorBase.HASH_SALT[%d & 0x1F];\n" +
				"code += this.f%d.hash(((" + typeName + ")value)." + accessStringForField(keyFields[i]) +
					");\n",
				i, i));
		}
		StringBuilder setReference = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			setReference.append(String.format(
				"this.f%d.setReference(((" + typeName + ")toCompare)." + accessStringForField(keyFields[i]) + ");\n",
				i));
		}
		StringBuilder equalToReference = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			equalToReference.append(String.format(
				"if (!this.f%d.equalToReference(((" + typeName + ")candidate)." +
				accessStringForField(keyFields[i]) + ")) {\n" +
				"	return false;\n" +
				"}\n", i));
		}
		StringBuilder compareToReference = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			compareToReference.append(String.format(
				"cmp = this.f%d.compareToReference(other.f%d);\n" +
				"if (cmp != 0) {\n" +
				"	return cmp;\n" +
				"}\n", i, i));
		}
		StringBuilder compareFields = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			compareFields.append(String.format(
				"cmp = f%d.compare(((" + typeName + ")first)." + accessStringForField(keyFields[i]) + "," +
				"((" + typeName + ")second)." + accessStringForField(keyFields[i]) + ");\n" +
				"if (cmp != 0) {\n" +
					"return cmp;\n" +
				"}\n", i));
		}
		StringBuilder putNormalizedKeys = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			putNormalizedKeys.append(String.format("if (%d >= numLeadingNormalizableKeys || numBytes <= 0) break;\n" +
				"len = normalizedKeyLengths[%d];\n" +
				"len = numBytes >= len ? len : numBytes;\n" +
				"f%d.putNormalizedKey(((" + typeName + ")value)." + accessStringForField(keyFields[i]) +
				", target, offset, len);\n" +
				"numBytes -= len;\n" +
				"offset += len;", i, i, i));
		}
		StringBuilder extractKeys = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			extractKeys.append(String.format(
				"localIndex += f%d.extractKeys(((" + typeName + ")record)." + accessStringForField(keyFields[i]) +
					", target, localIndex);\n", i));
		}
		Map<String, Object> root = new HashMap<>();
		root.put("packageName", packageName);
		root.put("className", className);
		root.put("serializerClassName", serializer.getClass().getCanonicalName());
		root.put("members", members.toString().split("\n"));
		root.put("initMembers", initMembers.toString().split("\n"));
		root.put("cloneMembers", cloneMembers.toString().split("\n"));
		root.put("flatComparators", flatComparators.toString().split("\n"));
		root.put("hashMembers", hashMembers.toString().split("\n"));
		root.put("normalizableKeys", normalizableKeys.toString().split("\n"));
		root.put("setReference", setReference.toString().split("\n"));
		root.put("equalToReference", equalToReference.toString().split("\n"));
		root.put("compareToReference", compareToReference.toString().split("\n"));
		root.put("compareFields", compareFields.toString().split("\n"));
		root.put("putNormalizedKeys", putNormalizedKeys.toString().split("\n"));
		root.put("extractKeys", extractKeys.toString().split("\n"));
		try {
			code = InstantiationUtil.getCodeFromTemplate("PojoComparatorTemplate.ftl", root);
		} catch (IOException e) {
			throw new RuntimeException("Unable to read template.", e);
		}
	}
}
