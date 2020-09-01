/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.datastream.typeinfo.python;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;

/**
 * A PickledByteArrayTypeInfo indicates that the data of this type is a generated primitive byte
 * array by pickle.
 */
public class PickledByteArrayTypeInfo extends TypeInformation<byte[]> {

	public static final PickledByteArrayTypeInfo PICKLED_BYTE_ARRAY_TYPE_INFO = new PickledByteArrayTypeInfo();

	protected PickledByteArrayTypeInfo() {
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
	public Class<byte[]> getTypeClass() {
		return byte[].class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<byte[]> createSerializer(ExecutionConfig config) {
		return BytePrimitiveArraySerializer.INSTANCE;
	}

	@Override
	public String toString() {
		return "PickledByteArrayTypeInfo";
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof PickledByteArrayTypeInfo;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof PickledByteArrayTypeInfo;
	}
}
