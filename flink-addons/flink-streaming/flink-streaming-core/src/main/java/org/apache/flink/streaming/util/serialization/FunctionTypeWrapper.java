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

package org.apache.flink.streaming.util.serialization;

import java.io.IOException;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class FunctionTypeWrapper<T> extends TypeWrapper<T> {
	private static final long serialVersionUID = 1L;

	private Function function;
	private Class<? extends Function> functionSuperClass;
	private int typeParameterNumber;

	public FunctionTypeWrapper(Function function, Class<? extends Function> functionSuperClass,
			int typeParameterNumber) {
		this.function = function;
		this.functionSuperClass = functionSuperClass;
		this.typeParameterNumber = typeParameterNumber;
		setTypeInfo();
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		setTypeInfo();
	}

	@Override
	protected void setTypeInfo() {
		if (typeParameterNumber != -1) {
			typeInfo = TypeExtractor.createTypeInfo(functionSuperClass, function.getClass(),
					typeParameterNumber, null, null);
		}
	}
}
