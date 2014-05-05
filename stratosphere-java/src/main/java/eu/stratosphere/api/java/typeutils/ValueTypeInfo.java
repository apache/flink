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

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.base.ValueComparator;
import eu.stratosphere.api.java.functions.InvalidTypesException;
import eu.stratosphere.api.java.typeutils.runtime.CopyableValueSerializer;
import eu.stratosphere.types.BooleanValue;
import eu.stratosphere.types.ByteValue;
import eu.stratosphere.types.CharValue;
import eu.stratosphere.types.CopyableValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.NullValue;
import eu.stratosphere.types.ShortValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;


public class ValueTypeInfo<T extends Value> extends TypeInformation<T> implements AtomicType<T> {

	private final Class<T> type;

	
	public ValueTypeInfo(Class<T> type) {
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
		return Key.class.isAssignableFrom(type);
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeSerializer<T> createSerializer() {
		if (CopyableValue.class.isAssignableFrom(type)) {
			return (TypeSerializer<T>) createCopyableSerializer(type.asSubclass(CopyableValue.class));
		}
		else {
			throw new UnsupportedOperationException("Serialization is not yet implemented for Value types that are not CopyableValue subclasses.");
		}
	}
	
	private static <X extends CopyableValue<X>> TypeSerializer<X> createCopyableSerializer(Class<X> clazz) {
		TypeSerializer<X> ser = new CopyableValueSerializer<X>(clazz);
		return ser;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public TypeComparator<T> createComparator(boolean sortOrderAscending) {
		if(type.isAssignableFrom(BooleanValue.class)) {
			return (TypeComparator<T>) new ValueComparator<BooleanValue>(sortOrderAscending, (Class<BooleanValue>) type);
		}
		else if(type.isAssignableFrom(ByteValue.class)) {
			return (TypeComparator<T>) new ValueComparator<ByteValue>(sortOrderAscending, (Class<ByteValue>) type);
		}
		else if(type.isAssignableFrom(CharValue.class)) {
			return (TypeComparator<T>) new ValueComparator<CharValue>(sortOrderAscending, (Class<CharValue>) type);
		}
		else if(type.isAssignableFrom(IntValue.class)) {
			return (TypeComparator<T>) new ValueComparator<IntValue>(sortOrderAscending, (Class<IntValue>) type);
		}
		else if(type.isAssignableFrom(LongValue.class)) {
			return (TypeComparator<T>) new ValueComparator<LongValue>(sortOrderAscending, (Class<LongValue>) type);
		}
		else if(type.isAssignableFrom(NullValue.class)) {
			return (TypeComparator<T>) new ValueComparator<NullValue>(sortOrderAscending, (Class<NullValue>) type);
		}
		else if(type.isAssignableFrom(ShortValue.class)) {
			return (TypeComparator<T>) new ValueComparator<ShortValue>(sortOrderAscending, (Class<ShortValue>) type);
		}
		else if(type.isAssignableFrom(StringValue.class)) {
			return (TypeComparator<T>) new ValueComparator<StringValue>(sortOrderAscending, (Class<StringValue>) type);
		}
		else if(type.isAssignableFrom(DoubleValue.class)) {
			return (TypeComparator<T>) new ValueComparator<DoubleValue>(sortOrderAscending, (Class<DoubleValue>) type);
		}
		else if(type.isAssignableFrom(FloatValue.class)) {
			return (TypeComparator<T>) new ValueComparator<FloatValue>(sortOrderAscending, (Class<FloatValue>) type);
		}
		else {
			throw new UnsupportedOperationException("Cannot create a comparator for a value type that is not a subtype of Key.");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return type.hashCode() ^ 0xd3a2646c;
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
