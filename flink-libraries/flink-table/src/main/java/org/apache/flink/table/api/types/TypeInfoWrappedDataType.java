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

package org.apache.flink.table.api.types;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.utils.TypeStringUtils;

/**
 * External type to wrap {@link TypeInformation}, may be it is a internally known
 * composite type(row, pojo, tuple, caseClass).
 */
public class TypeInfoWrappedDataType implements ExternalType {

	private TypeInformation typeInfo;
	private InternalType internalType;

	public TypeInfoWrappedDataType(TypeInformation typeInfo) {
		this.typeInfo = typeInfo;
		this.internalType = TypeConverters.createInternalTypeFromTypeInfo(typeInfo);
	}

	public TypeInformation getTypeInfo() {
		return typeInfo;
	}

	@Override
	public InternalType toInternalType() {
		return internalType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TypeInfoWrappedDataType that = (TypeInfoWrappedDataType) o;

		return typeInfo.equals(that.typeInfo);
	}

	@Override
	public int hashCode() {
		return typeInfo.hashCode();
	}

	@Override
	public String toString() {
		return TypeStringUtils.writeTypeInfo(typeInfo);
	}
}
