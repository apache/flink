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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TypeInformation} for the Multiset types of the Java API.
 *
 * @param <T> The type of the elements in the Multiset.
 */
@PublicEvolving
public final class MultisetTypeInfo<T> extends MapTypeInfo<T, Integer> {

	private static final long serialVersionUID = 1L;

	public MultisetTypeInfo(Class<T> elementTypeClass) {
		super(elementTypeClass, Integer.class);
	}

	public MultisetTypeInfo(TypeInformation<T> elementTypeInfo) {
		super(elementTypeInfo, BasicTypeInfo.INT_TYPE_INFO);
	}

	// ------------------------------------------------------------------------
	//  MultisetTypeInfo specific properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the type information for the elements contained in the Multiset
	 */
	public TypeInformation<T> getElementTypeInfo() {
		return getKeyTypeInfo();
	}

	@Override
	public String toString() {
		return "Multiset<" + getKeyTypeInfo() + '>';
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		else if (obj instanceof MultisetTypeInfo) {
			final MultisetTypeInfo<?> other = (MultisetTypeInfo<?>) obj;
			return other.canEqual(this) && getKeyTypeInfo().equals(other.getKeyTypeInfo());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return 31 * getKeyTypeInfo().hashCode() + 1;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj != null && obj.getClass() == getClass();
	}

	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <C> MultisetTypeInfo<C> getInfoFor(TypeInformation<C> componentInfo) {
		checkNotNull(componentInfo);

		return new MultisetTypeInfo<>(componentInfo);
	}
}
