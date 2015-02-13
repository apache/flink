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
 * WITHOUStreamRecord<?>WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class StreamWindowTypeInfo<T> extends TypeInformation<StreamWindow<T>> {

	private static final long serialVersionUID = 1L;
	TypeInformation<T> innerType;

	public StreamWindowTypeInfo(TypeInformation<T> innerType) {
		this.innerType = innerType;
	}

	public TypeInformation<T> getInnerType() {
		return innerType;
	}

	@Override
	public boolean isBasicType() {
		return innerType.isBasicType();
	}

	@Override
	public boolean isTupleType() {
		return innerType.isTupleType();
	}

	@Override
	public int getArity() {
		return innerType.getArity();
	}

	@Override
	public Class<StreamWindow<T>> getTypeClass() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isKeyType() {
		return innerType.isKeyType();
	}

	@Override
	public TypeSerializer<StreamWindow<T>> createSerializer(ExecutionConfig conf) {
		return new StreamWindowSerializer<T>(innerType, conf);
	}

	@Override
	public int getTotalFields() {
		return innerType.getTotalFields();
	}

}
