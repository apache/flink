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

package org.apache.flink.runtime.rpc.messages;

import org.apache.flink.util.Preconditions;

/**
 * Local rpc invocation message containing the remote procedure name, its parameter types and the
 * corresponding call arguments. This message will only be sent if the communication is local and,
 * thus, the message does not have to be serialized.
 */
public final class LocalRpcInvocation implements RpcInvocation {

	private final String methodName;
	private final Class<?>[] parameterTypes;
	private final Object[] args;

	private transient String toString;

	public LocalRpcInvocation(String methodName, Class<?>[] parameterTypes, Object[] args) {
		this.methodName = Preconditions.checkNotNull(methodName);
		this.parameterTypes = Preconditions.checkNotNull(parameterTypes);
		this.args = args;

		toString = null;
	}

	@Override
	public String getMethodName() {
		return methodName;
	}

	@Override
	public Class<?>[] getParameterTypes() {
		return parameterTypes;
	}

	@Override
	public Object[] getArgs() {
		return args;
	}

	@Override
	public String toString() {
		if (toString == null) {
			StringBuilder paramTypeStringBuilder = new StringBuilder(parameterTypes.length * 5);

			if (parameterTypes.length > 0) {
				paramTypeStringBuilder.append(parameterTypes[0].getSimpleName());

				for (int i = 1; i < parameterTypes.length; i++) {
					paramTypeStringBuilder
						.append(", ")
						.append(parameterTypes[i].getSimpleName());
				}
			}

			toString = "LocalRpcInvocation(" + methodName + '(' + paramTypeStringBuilder + "))";
		}

		return toString;
	}
}
