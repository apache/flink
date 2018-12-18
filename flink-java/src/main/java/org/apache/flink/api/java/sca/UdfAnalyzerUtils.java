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

package org.apache.flink.api.java.sca;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;

import org.apache.flink.shaded.asm5.org.objectweb.asm.ClassReader;
import org.apache.flink.shaded.asm5.org.objectweb.asm.Type;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.ClassNode;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.MethodNode;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.BasicValue;
import org.apache.flink.shaded.asm5.org.objectweb.asm.tree.analysis.Value;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class to work with {@link UdfAnalyzer}.
 */
@Internal
public final class UdfAnalyzerUtils {

	public static TaggedValue convertTypeInfoToTaggedValue(TaggedValue.Input input, TypeInformation<?> typeInfo,
			String flatFieldExpr, List<CompositeType.FlatFieldDescriptor> flatFieldDesc, int[] groupedKeys) {
		// java tuples & scala tuples
		if (typeInfo instanceof TupleTypeInfoBase) {
			final TupleTypeInfoBase<?> tupleTypeInfo = (TupleTypeInfoBase<?>) typeInfo;
			HashMap<String, TaggedValue> containerMapping = new HashMap<String, TaggedValue>();
			for (int i = 0; i < tupleTypeInfo.getArity(); i++) {
				final String fieldName;
				// java
				if (typeInfo instanceof TupleTypeInfo) {
					fieldName = "f" + i;
				}
				// scala
				else {
					fieldName = "_" + (i + 1);
				}
				containerMapping.put(fieldName,
						convertTypeInfoToTaggedValue(input,
								tupleTypeInfo.getTypeAt(i),
								(flatFieldExpr.length() > 0 ? flatFieldExpr + "." : "") + fieldName,
								tupleTypeInfo.getFlatFields(fieldName),
								groupedKeys));
			}
			return new TaggedValue(Type.getObjectType("java/lang/Object"), containerMapping);
		}
		// pojos
		else if (typeInfo instanceof PojoTypeInfo) {
			final PojoTypeInfo<?> pojoTypeInfo = (PojoTypeInfo<?>) typeInfo;
			HashMap<String, TaggedValue> containerMapping = new HashMap<String, TaggedValue>();
			for (int i = 0; i < pojoTypeInfo.getArity(); i++) {
				final String fieldName = pojoTypeInfo.getPojoFieldAt(i).getField().getName();
				containerMapping.put(fieldName,
						convertTypeInfoToTaggedValue(input,
								pojoTypeInfo.getTypeAt(i),
								(flatFieldExpr.length() > 0 ? flatFieldExpr + "." : "") + fieldName,
								pojoTypeInfo.getFlatFields(fieldName),
								groupedKeys));
			}
			return new TaggedValue(Type.getObjectType("java/lang/Object"), containerMapping);
		}
		// atomic
		boolean groupedField = false;
		if (groupedKeys != null && flatFieldDesc != null) {
			int flatFieldPos = flatFieldDesc.get(0).getPosition();
			for (int groupedKey : groupedKeys) {
				if (groupedKey == flatFieldPos) {
					groupedField = true;
					break;
				}
			}
		}

		return new TaggedValue(Type.getType(typeInfo.getTypeClass()), input, flatFieldExpr, groupedField,
				typeInfo.isBasicType() && typeInfo != BasicTypeInfo.DATE_TYPE_INFO);
	}

	/**
	 * @return array that contains the method node and the name of the class where
	 * the method node has been found
	 */
	public static Object[] findMethodNode(String internalClassName, Method method) {
		return findMethodNode(internalClassName, method.getName(), Type.getMethodDescriptor(method));
	}

	/**
	 * @return array that contains the method node and the name of the class where
	 * the method node has been found
	 */
	@SuppressWarnings("unchecked")
	public static Object[] findMethodNode(String internalClassName, String name, String desc) {
		InputStream stream = null;
		try {
			// iterate through hierarchy and search for method node /
			// class that really implements the method
			while (internalClassName != null) {
				stream = Thread.currentThread().getContextClassLoader()
						.getResourceAsStream(internalClassName.replace('.', '/') + ".class");
				ClassReader cr = new ClassReader(stream);
				final ClassNode cn = new ClassNode();
				cr.accept(cn, 0);
				for (MethodNode mn : (List<MethodNode>) cn.methods) {
					if (mn.name.equals(name) && mn.desc.equals(desc)) {
						return new Object[]{ mn , cr.getClassName()};
					}
				}
				internalClassName = cr.getSuperName();
			}
		}
		catch (IOException e) {
			throw new IllegalStateException("Method '" + name + "' could not be found", e);
		}
		finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (IOException e) {
					// best effort cleanup
				}
			}
		}
		throw new IllegalStateException("Method '" + name + "' could not be found");
	}

	public static boolean isTagged(Value value) {
		return value instanceof TaggedValue;
	}

	public static TaggedValue tagged(Value value) {
		return (TaggedValue) value;
	}

	/**
	 *
	 * @return returns whether a value of the list of values is or contains
	 * important dependencies (inputs or collectors) that require special analysis
	 * (e.g. to dig into a nested method). The first argument can be skipped e.g.
	 * in order to skip the "this" of non-static method arguments.
	 */
	public static boolean hasImportantDependencies(List<? extends BasicValue> values, boolean skipFirst) {
		for (BasicValue value : values) {
			if (skipFirst) {
				skipFirst = false;
				continue;
			}
			if (hasImportantDependencies(value)) {
				return true;
			}
		}
		return false;
	}

	/**
	 *
	 * @return returns whether a value is or contains important dependencies (inputs or collectors)
	 * that require special analysis (e.g. to dig into a nested method)
	 */
	public static boolean hasImportantDependencies(BasicValue bv) {
		if (!isTagged(bv)) {
			return false;
		}
		final TaggedValue value = tagged(bv);

		if (value.isInput() || value.isCollector()) {
			return true;
		}
		else if (value.canContainFields() && value.getContainerMapping() != null) {
			for (TaggedValue tv : value.getContainerMapping().values()) {
				if (hasImportantDependencies(tv)) {
					return true;
				}
			}
		}
		return false;
	}

	public static TaggedValue mergeInputs(List<TaggedValue> returnValues) {
		TaggedValue first = null;
		for (TaggedValue tv : returnValues) {
			if (first == null) {
				first = tv;
			}
			else if (!first.equals(tv)) {
				return null;
			}
		}
		return first;
	}

	public static TaggedValue mergeContainers(List<TaggedValue> returnValues) {
		if (returnValues.size() == 0) {
			return null;
		}

		Type returnType = null;

		// do intersections of field names
		Set<String> keys = null;
		for (TaggedValue tv : returnValues) {
			if (keys == null) {
				keys = new HashSet<String>(tv.getContainerMapping().keySet());
				returnType = tv.getType();
			}
			else {
				keys.retainAll(tv.getContainerMapping().keySet());
			}
		}

		// filter mappings with undefined state
		final HashMap<String, TaggedValue> resultMapping = new HashMap<String, TaggedValue>(keys.size());
		final List<String> filteredMappings = new ArrayList<String>(keys.size());
		for (TaggedValue tv : returnValues) {
			final Map<String, TaggedValue> cm = tv.getContainerMapping();
			for (String key : keys) {
				if (cm.containsKey(key)) {
					// add mapping with undefined state to filter
					if (!filteredMappings.contains(key) && cm.get(key) == null) {
						filteredMappings.add(key);
					}
					// add mapping to result mapping
					else if (!resultMapping.containsKey(key) && !filteredMappings.contains(key)) {
						resultMapping.put(key, cm.get(key));
					}
					// if mapping is present in result and filter,
					// remove it from result
					else if (resultMapping.containsKey(key)
							&& filteredMappings.contains(key)) {
						resultMapping.remove(key);
					}
					// if mapping is already present in result,
					// remove it and mark it as mapping with undefined state in filter
					else if (resultMapping.containsKey(key)
							&& !filteredMappings.contains(key)
							&& !cm.get(key).equals(resultMapping.get(key))) {
						filteredMappings.add(key);
						resultMapping.remove(key);
					}
				}
			}
		}

		// recursively merge contained mappings
		Iterator<Map.Entry<String, TaggedValue>> it = resultMapping.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, TaggedValue> entry = it.next();
			TaggedValue value = mergeReturnValues(Collections.singletonList(entry.getValue()));
			if (value == null) {
				it.remove();
			}
			else {
				entry.setValue(value);
			}
		}

		if (resultMapping.size() > 0) {
			return new TaggedValue(returnType, resultMapping);
		}
		return null;
	}

	public static TaggedValue mergeReturnValues(List<TaggedValue> returnValues) {
		if (returnValues.size() == 0 || returnValues.get(0) == null) {
			return null;
		}

		// check if either all inputs or all containers
		boolean allInputs = returnValues.get(0).isInput();
		for (TaggedValue tv : returnValues) {
			if (tv == null || tv.isInput() != allInputs) {
				return null;
			}
			// check if there are uninteresting values
			if (tv.canNotContainInput()) {
				return null;
			}
		}

		if (allInputs) {
			return mergeInputs(returnValues);
		}
		return mergeContainers(returnValues);
	}

	public static void removeUngroupedInputsFromContainer(TaggedValue value) {
		if (value.getContainerMapping() != null) {
			Iterator<Map.Entry<String, TaggedValue>> it = value.getContainerMapping().entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, TaggedValue> entry = it.next();
				if (entry.getValue() == null) {
					continue;
				}
				else if (entry.getValue().isInput() && !entry.getValue().isGrouped()) {
					it.remove();
				}
				else if (entry.getValue().canContainFields()) {
					removeUngroupedInputsFromContainer(entry.getValue());
				}
			}
		}
	}

	public static TaggedValue removeUngroupedInputs(TaggedValue value) {
		if (value.isInput()) {
			if (value.isGrouped()) {
				return value;
			}
		}
		else if (value.canContainFields()) {
			removeUngroupedInputsFromContainer(value);
			if (value.getContainerMapping() != null && value.getContainerMapping().size() > 0) {
				return value;
			}
		}
		return null;
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private UdfAnalyzerUtils() {
		throw new RuntimeException();
	}
}
