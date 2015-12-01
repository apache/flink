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

import com.google.common.base.Preconditions;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.WritableComparator;
import org.apache.flink.api.java.typeutils.runtime.WritableSerializer;
import org.apache.hadoop.io.Writable;

/**
 * Type information for data types that extend Hadoop's {@link Writable} interface. The Writable
 * interface defines the serialization and deserialization routines for the data type.
 *
 * @param <T> The type of the class represented by this type information.
 */
@Public
public class WritableTypeInfo<T extends Writable> extends TypeInformation<T> implements AtomicType<T> {
	
	private static final long serialVersionUID = 1L;
	
	private final Class<T> typeClass;

	@Experimental
	public WritableTypeInfo(Class<T> typeClass) {
		this.typeClass = Preconditions.checkNotNull(typeClass);

		Preconditions.checkArgument(
			Writable.class.isAssignableFrom(typeClass) && !typeClass.equals(Writable.class),
			"WritableTypeInfo can only be used for subclasses of " + Writable.class.getName());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	@Experimental
	public TypeComparator<T> createComparator(boolean sortOrderAscending, ExecutionConfig executionConfig) {
		if(Comparable.class.isAssignableFrom(typeClass)) {
			return new WritableComparator(sortOrderAscending, typeClass);
		}
		else {
			throw new UnsupportedOperationException("Cannot create Comparator for "+typeClass.getCanonicalName()+". " +
													"Class does not implement Comparable interface.");
		}
	}

	@Override
	@Experimental
	public boolean isBasicType() {
		return false;
	}

	@Override
	@Experimental
	public boolean isTupleType() {
		return false;
	}

	@Override
	@Experimental
	public int getArity() {
		return 1;
	}
	
	@Override
	@Experimental
	public int getTotalFields() {
		return 1;
	}

	@Override
	@Experimental
	public Class<T> getTypeClass() {
		return this.typeClass;
	}

	@Override
	@Experimental
	public boolean isKeyType() {
		return Comparable.class.isAssignableFrom(typeClass);
	}

	@Override
	@Experimental
	public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
		return new WritableSerializer<T>(typeClass);
	}
	
	@Override
	public String toString() {
		return "WritableType<" + typeClass.getName() + ">";
	}	
	
	@Override
	public int hashCode() {
		return typeClass.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof WritableTypeInfo) {
			@SuppressWarnings("unchecked")
			WritableTypeInfo<T> writableTypeInfo = (WritableTypeInfo<T>) obj;

			return writableTypeInfo.canEqual(this) &&
				typeClass == writableTypeInfo.typeClass;

		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof WritableTypeInfo;
	}
	
	// --------------------------------------------------------------------------------------------

	@Experimental
	static <T extends Writable> TypeInformation<T> getWritableTypeInfo(Class<T> typeClass) {
		if (Writable.class.isAssignableFrom(typeClass) && !typeClass.equals(Writable.class)) {
			return new WritableTypeInfo<T>(typeClass);
		}
		else {
			throw new InvalidTypesException("The given class is no subclass of " + Writable.class.getName());
		}
	}
	
}
