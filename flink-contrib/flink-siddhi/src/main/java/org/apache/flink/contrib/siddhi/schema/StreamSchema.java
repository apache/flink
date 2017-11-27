/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.schema;

import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Generic Field-based Stream Schema
 *
 * @param <T> Stream element type
 */
public class StreamSchema<T> implements Serializable {
	private static final Logger LOGGER = LoggerFactory.getLogger(StreamSchema.class);
	private final TypeInformation<T> typeInfo;
	private final int[] fieldIndexes;
	private final String[] fieldNames;
	private TypeInformation[] fieldTypes;
	private final StreamSerializer<T> streamSerializer;
	private TypeSerializer<T> typeSerializer;

	public StreamSchema(TypeInformation<T> typeInfo, String... fieldNames) {
		Preconditions.checkNotNull(fieldNames, "Field name is required");
		this.typeInfo = typeInfo;
		this.fieldNames = fieldNames;
		this.fieldIndexes = getFieldIndexes(typeInfo, fieldNames);
		this.fieldTypes = getFieldTypes(typeInfo, fieldIndexes, fieldNames);
		this.streamSerializer = new StreamSerializer<>(this);
	}

	public StreamSchema(TypeInformation<T> typeInfo, int[] fieldIndexes, String[] fieldNames) {
		this.typeInfo = typeInfo;
		this.fieldIndexes = fieldIndexes;
		this.fieldNames = fieldNames;
		this.fieldTypes = getFieldTypes(typeInfo, fieldIndexes, fieldNames);
		this.streamSerializer = new StreamSerializer<>(this);
	}

	public boolean isAtomicType() {
		return typeInfo instanceof AtomicType;
	}

	public boolean isTupleType() {
		return typeInfo instanceof TupleTypeInfo;
	}

	public boolean isPojoType() {
		return typeInfo instanceof PojoTypeInfo;
	}

	public boolean isCaseClassType() {
		return typeInfo instanceof CaseClassTypeInfo;
	}

	public boolean isCompositeType() {
		return typeInfo instanceof CompositeType;
	}

	private <E> int[] getFieldIndexes(TypeInformation<E> typeInfo, String... fieldNames) {
		int[] result;
		if (isAtomicType()) {
			result = new int[]{0};
		} else if (isTupleType()) {
			result = new int[fieldNames.length];
			for (int i = 0; i < fieldNames.length; i++) {
				result[i] = i;
			}
		} else if (isPojoType()) {
			result = new int[fieldNames.length];
			for (int i = 0; i < fieldNames.length; i++) {
				int index = ((PojoTypeInfo) typeInfo).getFieldIndex(fieldNames[i]);
				if (index < 0) {
					throw new IllegalArgumentException(fieldNames[i] + " is not a field of type " + typeInfo);
				}
				result[i] = index;
			}
		} else if (isCaseClassType()) {
			result = new int[fieldNames.length];
			for (int i = 0; i < fieldNames.length; i++) {
				int index = ((CaseClassTypeInfo) typeInfo).getFieldIndex(fieldNames[i]);
				if (index < 0) {
					throw new IllegalArgumentException(fieldNames[i] + " is not a field of type " + typeInfo);
				}
				result[i] = index;
			}
		} else {
			throw new IllegalArgumentException("Failed to get field index from " + typeInfo);
		}
		return result;
	}


	private <E> TypeInformation[] getFieldTypes(TypeInformation<E> typeInfo, int[] fieldIndexes, String[] fieldNames) {
		TypeInformation[] fieldTypes;
		if (isCompositeType()) {
			CompositeType cType = (CompositeType) typeInfo;
			if (fieldNames.length != cType.getArity()) {
//				throw new IllegalArgumentException("Arity of type (" + cType.getFieldNames().length+ ") " +
//					"not equal to number of field names " + fieldNames.length + ".");
				LOGGER.warn("Arity of type (" + cType.getFieldNames().length + ") " +
					"not equal to number of field names " + fieldNames.length + ".");
			}
			fieldTypes = new TypeInformation[fieldIndexes.length];
			for (int i = 0; i < fieldIndexes.length; i++) {
				fieldTypes[i] = cType.getTypeAt(fieldIndexes[i]);
			}
		} else if (isAtomicType()) {
			if (fieldIndexes.length != 1 || fieldIndexes[0] != 0) {
				throw new IllegalArgumentException(
					"Non-composite input type may have only a single field and its index must be 0.");
			}
			fieldTypes = new TypeInformation[]{typeInfo};
		} else {
			throw new IllegalArgumentException(
				"Illegal input type info"
			);
		}
		return fieldTypes;
	}

	public TypeInformation<T> getTypeInfo() {
		return typeInfo;
	}

	public int[] getFieldIndexes() {
		return fieldIndexes;
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public TypeInformation[] getFieldTypes() {
		return fieldTypes;
	}

	public StreamSerializer<T> getStreamSerializer() {
		return streamSerializer;
	}

	public TypeSerializer<T> getTypeSerializer() {
		return typeSerializer;
	}

	public void setTypeSerializer(TypeSerializer<T> typeSerializer) {
		this.typeSerializer = typeSerializer;
	}
}
