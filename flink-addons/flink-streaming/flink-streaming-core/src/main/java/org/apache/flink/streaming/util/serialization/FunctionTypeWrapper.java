/**
 *
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
 *
 */

package org.apache.flink.streaming.util.serialization;

import java.io.IOException;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class FunctionTypeWrapper<IN1, IN2, OUT> extends TypeSerializerWrapper<IN1, IN2, OUT> {
	private static final long serialVersionUID = 1L;

	private Function function;
	private Class<? extends Function> functionSuperClass;
	private int inTypeParameter1;
	private int inTypeParameter2;
	private int outTypeParameter;

	public FunctionTypeWrapper(Function function, Class<? extends Function> functionSuperClass,
			int inTypeParameter1, int inTypeParameter2, int outTypeParameter) {
		this.function = function;
		this.functionSuperClass = functionSuperClass;
		this.inTypeParameter1 = inTypeParameter1;
		this.inTypeParameter2 = inTypeParameter2;
		this.outTypeParameter = outTypeParameter;
		setTypeInfo();
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		setTypeInfo();
	}

	@Override
	protected void setTypeInfo() {
		if (inTypeParameter1 != -1) {
			inTypeInfo1 = TypeExtractor.createTypeInfo(functionSuperClass, function.getClass(),
					inTypeParameter1, null, null);
		}

		if (inTypeParameter2 != -1) {
			inTypeInfo2 = TypeExtractor.createTypeInfo(functionSuperClass, function.getClass(),
					inTypeParameter2, null, null);
		}

		if (outTypeParameter != -1) {
			outTypeInfo = TypeExtractor.createTypeInfo(functionSuperClass, function.getClass(),
					outTypeParameter, null, null);
		}
	}
}
