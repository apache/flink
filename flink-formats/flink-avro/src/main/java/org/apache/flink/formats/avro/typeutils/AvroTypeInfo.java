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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.apache.avro.specific.SpecificRecordBase;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Special type information to generate a special AvroTypeInfo for Avro POJOs (implementing SpecificRecordBase, the typed Avro POJOs)
 *
 * <p>Proceeding: It uses a regular pojo type analysis and replaces all {@code GenericType<CharSequence>} with a {@code GenericType<avro.Utf8>}.
 * All other types used by Avro are standard Java types.
 * Only strings are represented as CharSequence fields and represented as Utf8 classes at runtime.
 * CharSequence is not comparable. To make them nicely usable with field expressions, we replace them here
 * by generic type infos containing Utf8 classes (which are comparable),
 *
 * <p>This class is checked by the AvroPojoTest.
 */
public class AvroTypeInfo<T extends SpecificRecordBase> extends PojoTypeInfo<T> {

	private static final long serialVersionUID = 1L;

	private static final ConcurrentHashMap<Thread, Boolean> IN_BACKWARDS_COMPATIBLE_MODE = new ConcurrentHashMap<>();

	private final boolean useBackwardsCompatibleSerializer;

	/**
	 * Creates a new Avro type info for the given class.
	 */
	public AvroTypeInfo(Class<T> typeClass) {
		this(typeClass, false);
	}

	/**
	 * Creates a new Avro type info for the given class.
	 *
	 * <p>This constructor takes a flag to specify whether a serializer
	 * that is backwards compatible with PoJo-style serialization of Avro types should be used.
	 * That is only necessary, if one has a Flink 1.3 (or earlier) savepoint where Avro types
	 * were stored in the checkpointed state. New Flink programs will never need this.
	 */
	public AvroTypeInfo(Class<T> typeClass, boolean useBackwardsCompatibleSerializer) {
		super(typeClass, generateFieldsFromAvroSchema(typeClass, useBackwardsCompatibleSerializer));

		final Boolean modeOnStack = IN_BACKWARDS_COMPATIBLE_MODE.get(Thread.currentThread());
		this.useBackwardsCompatibleSerializer = modeOnStack == null ?
				useBackwardsCompatibleSerializer : modeOnStack;
	}

	@Override
	@SuppressWarnings("deprecation")
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		return useBackwardsCompatibleSerializer ?
				new BackwardsCompatibleAvroSerializer<>(getTypeClass()) :
				new AvroSerializer<>(getTypeClass());
	}

	@SuppressWarnings("unchecked")
	@Internal
	public static <T extends SpecificRecordBase> List<PojoField> generateFieldsFromAvroSchema(
			Class<T> typeClass,
			boolean useBackwardsCompatibleSerializer) {

		final Thread currentThread = Thread.currentThread();
		final boolean entryPoint =
				IN_BACKWARDS_COMPATIBLE_MODE.putIfAbsent(currentThread, useBackwardsCompatibleSerializer) == null;

		try {
			PojoTypeExtractor pte = new PojoTypeExtractor();
			ArrayList<Type> typeHierarchy = new ArrayList<>();
			typeHierarchy.add(typeClass);
			TypeInformation ti = pte.analyzePojo(typeClass, typeHierarchy, null, null, null);

			if (!(ti instanceof PojoTypeInfo)) {
				throw new IllegalStateException("Expecting type to be a PojoTypeInfo");
			}
			PojoTypeInfo pti =  (PojoTypeInfo) ti;
			List<PojoField> newFields = new ArrayList<>(pti.getTotalFields());

			for (int i = 0; i < pti.getArity(); i++) {
				PojoField f = pti.getPojoFieldAt(i);
				TypeInformation newType = f.getTypeInformation();
				// check if type is a CharSequence
				if (newType instanceof GenericTypeInfo) {
					if ((newType).getTypeClass().equals(CharSequence.class)) {
						// replace the type by a org.apache.avro.util.Utf8
						newType = new GenericTypeInfo(org.apache.avro.util.Utf8.class);
					}
				}
				PojoField newField = new PojoField(f.getField(), newType);
				newFields.add(newField);
			}
			return newFields;
		}
		finally {
			if (entryPoint) {
				IN_BACKWARDS_COMPATIBLE_MODE.remove(currentThread);
			}
		}
	}

	private static class PojoTypeExtractor extends TypeExtractor {
		private PojoTypeExtractor() {
			super();
		}

		@Override
		public <OUT, IN1, IN2> TypeInformation<OUT> analyzePojo(Class<OUT> clazz, ArrayList<Type> typeHierarchy,
				ParameterizedType parameterizedType, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
			return super.analyzePojo(clazz, typeHierarchy, parameterizedType, in1Type, in2Type);
		}
	}
}
