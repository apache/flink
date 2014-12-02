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

package org.apache.flink.api.java.typeutils;

import java.util.Arrays;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
//CHECKSTYLE.OFF: AvoidStarImport - Needed for TupleGenerator
import org.apache.flink.api.java.tuple.*;
//CHECKSTYLE.ON: AvoidStarImport
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;




public final class TupleTypeInfo<T extends Tuple> extends TupleTypeInfoBase<T> {
	
	@SuppressWarnings("unchecked")
	public TupleTypeInfo(TypeInformation<?>... types) {
		this((Class<T>) Tuple.getTupleClass(types.length), types);
	}

	public TupleTypeInfo(Class<T> tupleType, TypeInformation<?>... types) {
		super(tupleType, types);
		if (types == null || types.length == 0 || types.length > Tuple.MAX_ARITY) {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public TupleSerializer<T> createSerializer() {
		TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[getArity()];
		for (int i = 0; i < types.length; i++) {
			fieldSerializers[i] = types[i].createSerializer();
		}
		
		Class<T> tupleClass = getTypeClass();
		
		return new TupleSerializer<T>(tupleClass, fieldSerializers);
	}
	
	/**
	 * Comparator creation
	 */
	private TypeComparator<?>[] fieldComparators;
	private int[] logicalKeyFields;
	private int comparatorHelperIndex = 0;
	
	@Override
	protected void initializeNewComparator(int localKeyCount) {
		fieldComparators = new TypeComparator<?>[localKeyCount];
		logicalKeyFields = new int[localKeyCount];
		comparatorHelperIndex = 0;
	}

	@Override
	protected void addCompareField(int fieldId, TypeComparator<?> comparator) {
		fieldComparators[comparatorHelperIndex] = comparator;
		logicalKeyFields[comparatorHelperIndex] = fieldId;
		comparatorHelperIndex++;
	}

	@Override
	protected TypeComparator<T> getNewComparator() {
		@SuppressWarnings("rawtypes")
		final TypeComparator[] finalFieldComparators = Arrays.copyOf(fieldComparators, comparatorHelperIndex);
		final int[] finalLogicalKeyFields = Arrays.copyOf(logicalKeyFields, comparatorHelperIndex);
		//final TypeSerializer[] finalFieldSerializers = Arrays.copyOf(fieldSerializers, comparatorHelperIndex);
		// create the serializers for the prefix up to highest key position
		int maxKey = 0;
		for(int key : finalLogicalKeyFields) {
			maxKey = Math.max(maxKey, key);
		}
		TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[maxKey + 1];
		for (int i = 0; i <= maxKey; i++) {
			fieldSerializers[i] = types[i].createSerializer();
		}
		if(finalFieldComparators.length == 0 || finalLogicalKeyFields.length == 0 || fieldSerializers.length == 0 
				|| finalFieldComparators.length != finalLogicalKeyFields.length) {
			throw new IllegalArgumentException("Tuple comparator creation has a bug");
		}
		return new TupleComparator<T>(finalLogicalKeyFields, finalFieldComparators, fieldSerializers);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TupleTypeInfo) {
			@SuppressWarnings("unchecked")
			TupleTypeInfo<T> other = (TupleTypeInfo<T>) obj;
			return ((this.tupleType == null && other.tupleType == null) || this.tupleType.equals(other.tupleType)) &&
					Arrays.deepEquals(this.types, other.types);
			
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return this.types.hashCode() ^ Arrays.deepHashCode(this.types);
	}
	
	@Override
	public String toString() {
		return "Java " + super.toString();
	}

	// --------------------------------------------------------------------------------------------
	
	public static <X extends Tuple> TupleTypeInfo<X> getBasicTupleTypeInfo(Class<?>... basicTypes) {
		if (basicTypes == null || basicTypes.length == 0) {
			throw new IllegalArgumentException();
		}
		
		TypeInformation<?>[] infos = new TypeInformation<?>[basicTypes.length];
		for (int i = 0; i < infos.length; i++) {
			Class<?> type = basicTypes[i];
			if (type == null) {
				throw new IllegalArgumentException("Type at position " + i + " is null.");
			}
			
			TypeInformation<?> info = BasicTypeInfo.getInfoFor(type);
			if (info == null) {
				throw new IllegalArgumentException("Type at position " + i + " is not a basic type.");
			}
			infos[i] = info;
		}
		
		@SuppressWarnings("unchecked")
		TupleTypeInfo<X> tupleInfo = (TupleTypeInfo<X>) new TupleTypeInfo<Tuple>(infos);
		return tupleInfo;
	}
}
