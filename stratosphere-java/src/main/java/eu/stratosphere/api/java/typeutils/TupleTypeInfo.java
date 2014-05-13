/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils;

import java.util.Arrays;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
//CHECKSTYLE.OFF: AvoidStarImport - Needed for TupleGenerator
import eu.stratosphere.api.java.tuple.*;
//CHECKSTYLE.ON: AvoidStarImport
import eu.stratosphere.api.java.typeutils.runtime.TupleComparator;
import eu.stratosphere.api.java.typeutils.runtime.TupleSerializer;
import eu.stratosphere.api.java.typeutils.runtime.TupleLeadingFieldComparator;


public class TupleTypeInfo<T extends Tuple> extends TypeInformation<T> implements CompositeType<T> {
	
	private final TypeInformation<?>[] types;
	
	private final Class<T> tupleType;
	
	
	public TupleTypeInfo(TypeInformation<?>... types) {
		if (types == null || types.length == 0 || types.length > Tuple.MAX_ARITY) {
			throw new IllegalArgumentException();
		}

		@SuppressWarnings("unchecked")
		Class<T> typeClass = (Class<T>) CLASSES[types.length - 1];
		
		this.types = types;
		this.tupleType = typeClass;
	}
	
	public TupleTypeInfo(Class<T> tupleType, TypeInformation<?>... types) {
		if (types == null || types.length == 0 || types.length > Tuple.MAX_ARITY) {
			throw new IllegalArgumentException();
		}
		
		this.tupleType = tupleType;
		this.types = types;
	}
	
	
	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return true;
	}

	@Override
	public int getArity() {
		return types.length;
	}

	@Override
	public Class<T> getTypeClass() {
		return tupleType;
	}

	
	public <X> TypeInformation<X> getTypeAt(int pos) {
		if (pos < 0 || pos >= this.types.length) {
			throw new IndexOutOfBoundsException();
		}

		@SuppressWarnings("unchecked")
		TypeInformation<X> typed = (TypeInformation<X>) this.types[pos];
		return typed;
	}
	
	@Override
	public boolean isKeyType() {
		return false;
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
	
	@Override
	public TypeComparator<T> createComparator(int[] logicalKeyFields, boolean[] orders) {
		// sanity checks
		if (logicalKeyFields == null || orders == null || logicalKeyFields.length != orders.length ||
				logicalKeyFields.length > types.length)
		{
			throw new IllegalArgumentException();
		}
		
		if (logicalKeyFields.length == 1 && logicalKeyFields[0] == 0) {
			return createLeadingFieldComparator(orders[0], types[0]);
		}
				
		int maxKey = logicalKeyFields[0];
		for(int key : logicalKeyFields){
			if (key > maxKey){
				maxKey = key;
			}
		}
		
		boolean[] isKey = new boolean[maxKey + 1];
		for(int key:logicalKeyFields){
			isKey[key]=true;
		}
		
		// create the comparators for the individual fields
		TypeComparator<?>[] fieldComparators = new TypeComparator<?>[logicalKeyFields.length];
		TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[maxKey + 1 -logicalKeyFields.length];
		
		int cIndex=0;
		int sIndex=0;
		for (int i = 0; i < maxKey + 1; i++) {
			if(isKey[i]){
				if (types[i].isKeyType() && types[i] instanceof AtomicType) {
				fieldComparators[cIndex] = ((AtomicType<?>) types[i]).createComparator(orders[cIndex]);
				cIndex++;
			} else {
				throw new IllegalArgumentException("The field at position " + i + " (" + types[i] + ") is no atomic key type.");
			}
			}else{
				fieldSerializers[sIndex] = types[i].createSerializer();
				sIndex++;
			}
		}		
		return new TupleComparator<T>(logicalKeyFields, fieldComparators, fieldSerializers);
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
		StringBuilder bld = new StringBuilder("Tuple");
		bld.append(types.length).append('<');
		bld.append(types[0]);
		
		for (int i = 1; i < types.length; i++) {
			bld.append(", ").append(types[i]);
		}
		
		bld.append('>');
		return bld.toString();
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
	
	// --------------------------------------------------------------------------------------------	
	// The following lines are generated.
	// --------------------------------------------------------------------------------------------
	
	// BEGIN_OF_TUPLE_DEPENDENT_CODE	
	// GENERATED FROM eu.stratosphere.api.java.tuple.TupleGenerator.
	private static final Class<?>[] CLASSES = new Class<?>[] {
		Tuple1.class, Tuple2.class, Tuple3.class, Tuple4.class, Tuple5.class, Tuple6.class, Tuple7.class, Tuple8.class, Tuple9.class, Tuple10.class, Tuple11.class, Tuple12.class, Tuple13.class, Tuple14.class, Tuple15.class, Tuple16.class, Tuple17.class, Tuple18.class, Tuple19.class, Tuple20.class, Tuple21.class, Tuple22.class, Tuple23.class, Tuple24.class, Tuple25.class
	};
	// END_OF_TUPLE_DEPENDENT_CODE
	
	
	private static final <T extends Tuple, K> TypeComparator<T> createLeadingFieldComparator(boolean ascending, TypeInformation<?> info) {
		if (!(info.isKeyType() && info instanceof AtomicType)) {
			throw new IllegalArgumentException("The field at position 0 (" + info + ") is no atomic key type.");
		}
		
		@SuppressWarnings("unchecked")
		AtomicType<K> typedInfo = (AtomicType<K>) info;
		return new TupleLeadingFieldComparator<T, K>(typedInfo.createComparator(ascending));
	}
}
