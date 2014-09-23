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

import org.apache.flink.api.common.typeinfo.CompositeType;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public abstract class TupleTypeInfoBase<T> extends TypeInformation<T> implements CompositeType<T> {
	
	protected final TypeInformation<?>[] types;
	
	protected final Class<T> tupleType;
	
	public TupleTypeInfoBase(Class<T> tupleType, TypeInformation<?>... types) {
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
		return isValidKeyType(this);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TupleTypeInfoBase) {
			@SuppressWarnings("unchecked")
			TupleTypeInfoBase<T> other = (TupleTypeInfoBase<T>) obj;
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

	private boolean isValidKeyType(TypeInformation<?> typeInfo) {
		if(typeInfo instanceof TupleTypeInfoBase) {
			TupleTypeInfoBase<?> tupleType = ((TupleTypeInfoBase<?>)typeInfo);
			for(int i=0;i<tupleType.getArity();i++) {
				if (!isValidKeyType(tupleType.getTypeAt(i))) {
					return false;
				}
			}
			return true;
		} else  {
			return typeInfo.isKeyType();
		}
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
}
