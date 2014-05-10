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

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.java.functions.InvalidTypesException;
import eu.stratosphere.api.java.typeutils.runtime.CopyableValueComparator;
import eu.stratosphere.api.java.typeutils.runtime.CopyableValueSerializer;
import eu.stratosphere.api.java.typeutils.runtime.ValueComparator;
import eu.stratosphere.api.java.typeutils.runtime.ValueSerializer;
import eu.stratosphere.types.CopyableValue;
import eu.stratosphere.types.Value;


public class ValueTypeInfo<T extends Value> extends TypeInformation<T> implements AtomicType<T> {

	private final Class<T> type;

	
	public ValueTypeInfo(Class<T> type) {
		if (type == null) {
			throw new NullPointerException();
		}
		if (!Value.class.isAssignableFrom(type)) {
			throw new IllegalArgumentException("ValueTypeInfo can only be used for subclasses of " + Value.class.getName());
		}
		
		this.type = type;
	}
	
	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public Class<T> getTypeClass() {
		return this.type;
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}
	
	@Override
	public boolean isKeyType() {
		return Comparable.class.isAssignableFrom(type);
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeSerializer<T> createSerializer() {
		if (CopyableValue.class.isAssignableFrom(type)) {
			return (TypeSerializer<T>) createCopyableValueSerializer(type.asSubclass(CopyableValue.class));
		}
		else {
			return new ValueSerializer<T>(type);
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public TypeComparator<T> createComparator(boolean sortOrderAscending) {
		if (!isKeyType()) {
			throw new RuntimeException("The type " + type.getName() + " is not Comparable.");
		}
		
		if (CopyableValue.class.isAssignableFrom(type)) {
			return (TypeComparator<T>) new ValueComparator(sortOrderAscending, type);
		}
		else {
			return (TypeComparator<T>) new CopyableValueComparator(sortOrderAscending, type);
		}
	}
	
	// utility method to summon the necessary bound
	private static <X extends CopyableValue<X>> CopyableValueSerializer<X> createCopyableValueSerializer(Class<X> clazz) {
		return new CopyableValueSerializer<X>(clazz);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return this.type.hashCode() ^ 0xd3a2646c;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() == ValueTypeInfo.class) {
			return type == ((ValueTypeInfo<?>) obj).type;
		} else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return "ValueType<" + type.getName() + ">";
	}
	
	// --------------------------------------------------------------------------------------------
	
	static final <X extends Value> TypeInformation<X> getValueTypeInfo(Class<X> typeClass) {
		if (Value.class.isAssignableFrom(typeClass) && !typeClass.equals(Value.class)) {
			return new ValueTypeInfo<X>(typeClass);
		}
		else {
			throw new InvalidTypesException("The given class is no subclass of " + Value.class.getName());
		}
	}
}
