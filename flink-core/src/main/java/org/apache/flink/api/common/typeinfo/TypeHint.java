/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * A utility class for describing generic types. It can be used to obtain a type information via:
 *
 * <pre>{@code
 * TypeInformation<Tuple2<String, Long>> info = TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){});
 * }</pre>
 * or
 * <pre>{@code
 * TypeInformation<Tuple2<String, Long>> info = new TypeHint<Tuple2<String, Long>>(){}.getTypeInfo();
 * }</pre>
 *
 * @param <T> The type information to hint.
 */
@Public
public abstract class TypeHint<T> {

	/** The type information described by the hint. */
	private final TypeInformation<T> typeInfo;

	/**
	 * Creates a hint for the generic type in the class signature.
	 */
	public TypeHint() {
		try {
			this.typeInfo = TypeExtractor.createTypeInfo(
					this, TypeHint.class, getClass(), 0);
		}
		catch (InvalidTypesException e) {
			throw new FlinkRuntimeException("The TypeHint is using a generic variable." +
					"This is not supported, generic types must be fully specified for the TypeHint.");
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the type information described by this TypeHint.
	 * @return The type information described by this TypeHint.
	 */
	public TypeInformation<T> getTypeInfo() {
		return typeInfo;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return typeInfo.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
			obj instanceof TypeHint && this.typeInfo.equals(((TypeHint<?>) obj).typeInfo);
	}

	@Override
	public String toString() {
		return "TypeHint: " + typeInfo;
	}
}
