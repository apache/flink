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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.EnumComparator;
import org.apache.flink.api.common.typeutils.base.EnumSerializer;

/**
 * A {@link TypeInformation} for java enumeration types. 
 *
 * @param <T> The type represented by this type information.
 */
public class EnumTypeInfo<T extends Enum<T>> extends TypeInformation<T> implements AtomicType<T> {

	private static final long serialVersionUID = 8936740290137178660L;
	
	private final Class<T> typeClass;

	public EnumTypeInfo(Class<T> typeClass) {
		Preconditions.checkNotNull(typeClass, "Enum type class must not be null.");

		if (!Enum.class.isAssignableFrom(typeClass) ) {
			throw new IllegalArgumentException("EnumTypeInfo can only be used for subclasses of " + Enum.class.getName());
		}

		this.typeClass = typeClass;
	}

	@Override
	public TypeComparator<T> createComparator(boolean sortOrderAscending, ExecutionConfig executionConfig) {
		return new EnumComparator<T>(sortOrderAscending);
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
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<T> getTypeClass() {
		return this.typeClass;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
		return new EnumSerializer<T>(typeClass);
	}
	
	// ------------------------------------------------------------------------
	//  Standard utils
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "EnumTypeInfo<" + typeClass.getName() + ">";
	}	
	
	@Override
	public int hashCode() {
		return typeClass.hashCode();
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof EnumTypeInfo;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof EnumTypeInfo) {
			@SuppressWarnings("unchecked")
			EnumTypeInfo<T> enumTypeInfo = (EnumTypeInfo<T>) obj;

			return enumTypeInfo.canEqual(this) &&
				typeClass == enumTypeInfo.typeClass;
		} else {
			return false;
		}
	}
}
