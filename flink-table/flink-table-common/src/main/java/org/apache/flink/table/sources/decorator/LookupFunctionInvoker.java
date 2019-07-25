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

package org.apache.flink.table.sources.decorator;

import org.apache.flink.table.functions.TableFunction;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * LookupFunctionInvoker is mainly to handle the parameter type.
 */
class LookupFunctionInvoker implements InvocationHandler {

	private final TableFunction tableFunction;
	private volatile Method realMethod = null;

	public LookupFunctionInvoker(TableFunction tableFunction) {
		this.tableFunction = tableFunction;
	}

	public Evaluation getProxy() {
		return (Evaluation) Proxy.newProxyInstance(tableFunction.getClass().getClassLoader(),
			new Class[]{Evaluation.class},
			this);
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		//Proxy will invoke toString method in some case.
		if (!method.getName().equals("eval")) {
			return method.invoke(tableFunction, args);
		}
		//NOTICE: In practice, no need to support multi eval functions in a TableFunction.
		if (realMethod == null) {
			realMethod = tableFunction.getClass().getMethod(method.getName(), method.getParameterTypes());
		}
		return realMethod.invoke(tableFunction, args);
	}

	/**
	 * dynamic interface. now only support Object... and Object.
	 */
	public interface Evaluation {
		void eval(Object... args);

		void eval(Object arg);
	}
}
