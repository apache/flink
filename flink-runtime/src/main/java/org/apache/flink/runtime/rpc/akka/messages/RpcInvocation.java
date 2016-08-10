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

package org.apache.flink.runtime.rpc.akka.messages;

import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Rpc invocation message containing the remote procedure name, its parameter types and the
 * corresponding call arguments.
 */
public final class RpcInvocation implements Serializable {
	private static final long serialVersionUID = -7058254033460536037L;

	private final String methodName;
	private final Class<?>[] parameterTypes;
	private transient Object[] args;

	public RpcInvocation(String methodName, Class<?>[] parameterTypes, Object[] args) {
		this.methodName = Preconditions.checkNotNull(methodName);
		this.parameterTypes = Preconditions.checkNotNull(parameterTypes);
		this.args = args;
	}

	public String getMethodName() {
		return methodName;
	}

	public Class<?>[] getParameterTypes() {
		return parameterTypes;
	}

	public Object[] getArgs() {
		return args;
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();

		if (args != null) {
			// write has args true
			oos.writeBoolean(true);

			for (int i = 0; i < args.length; i++) {
				try {
					oos.writeObject(args[i]);
				} catch (IOException e) {
					Class<?> argClass = args[i].getClass();

					throw new IOException("Could not write " + i + "th argument of method " +
						methodName + ". The argument type is " + argClass + ". " +
						"Make sure that this type is serializable.", e);
				}
			}
		} else {
			// write has args false
			oos.writeBoolean(false);
		}
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();

		boolean hasArgs = ois.readBoolean();

		if (hasArgs) {
			int numberArguments = parameterTypes.length;

			args = new Object[numberArguments];

			for (int i = 0; i < numberArguments; i++) {
				args[i] = ois.readObject();
			}
		} else {
			args = null;
		}
	}
}
