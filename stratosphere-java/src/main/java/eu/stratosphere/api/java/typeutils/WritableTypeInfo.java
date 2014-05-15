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

import org.apache.hadoop.io.Writable;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.java.functions.InvalidTypesException;
import eu.stratosphere.api.java.typeutils.runtime.WritableComparator;
import eu.stratosphere.api.java.typeutils.runtime.WritableSerializer;

public class WritableTypeInfo<T extends Writable> extends TypeInformation<T> implements AtomicType<T> {
	
	private final Class<T> typeClass;
	
	public WritableTypeInfo(Class<T> typeClass) {
		if (typeClass == null) {
			throw new NullPointerException();
		}
		if (!Writable.class.isAssignableFrom(typeClass) || typeClass == Writable.class) {
			throw new IllegalArgumentException("WritableTypeInfo can only be used for subclasses of " + Writable.class.getName());
		}
		this.typeClass = typeClass;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public TypeComparator<T> createComparator(boolean sortOrderAscending) {
		if(Comparable.class.isAssignableFrom(typeClass)) {
			return new WritableComparator(sortOrderAscending, typeClass);
		}
		else {
			throw new UnsupportedOperationException("Cannot create Comparator for "+typeClass.getCanonicalName()+". " +
													"Class does not implement Comparable interface.");
		}
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
	public int getArity() {
		return 1;
	}

	@Override
	public Class<T> getTypeClass() {
		return this.typeClass;
	}

	@Override
	public boolean isKeyType() {
		return Comparable.class.isAssignableFrom(typeClass);
	}

	@Override
	public TypeSerializer<T> createSerializer() {
		return new WritableSerializer<T>(typeClass);
	}
	
	@Override
	public String toString() {
		return "WritableType<" + typeClass.getName() + ">";
	}	
	
	// --------------------------------------------------------------------------------------------
	
	static final <T extends Writable> TypeInformation<T> getWritableTypeInfo(Class<T> typeClass) {
		if (Writable.class.isAssignableFrom(typeClass) && !typeClass.equals(Writable.class)) {
			return new WritableTypeInfo<T>(typeClass);
		}
		else {
			throw new InvalidTypesException("The given class is no subclass of " + Writable.class.getName());
		}
	}
	
}
