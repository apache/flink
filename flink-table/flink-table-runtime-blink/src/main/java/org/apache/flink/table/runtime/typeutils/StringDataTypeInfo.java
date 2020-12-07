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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.StringData;

/**
 * TypeInformation for {@link StringData}.
 */
@Internal
public class StringDataTypeInfo extends TypeInformation<StringData> {

	private static final long serialVersionUID = 1L;
	public static final StringDataTypeInfo INSTANCE = new StringDataTypeInfo();

	private StringDataTypeInfo() {}

	@Override
	public boolean isBasicType() {
		return true;
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
	public Class<StringData> getTypeClass() {
		return StringData.class;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<StringData> createSerializer(ExecutionConfig config) {
		return StringDataSerializer.INSTANCE;
	}

	@Override
	public String toString() {
		return "StringData";
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof StringDataTypeInfo;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof StringDataTypeInfo;
	}
}
