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

package org.apache.flink.runtime.rpc;

import java.util.HashSet;
import java.util.Set;

/**
 * Utility functions for Flink's RPC implementation
 */
public class RpcUtils {
	/**
	 * Extracts all {@link RpcGateway} interfaces implemented by the given clazz.
	 *
	 * @param clazz from which to extract the implemented RpcGateway interfaces
	 * @return A set of all implemented RpcGateway interfaces
	 */
	public static Set<Class<? extends RpcGateway>> extractImplementedRpcGateways(Class<?> clazz) {
		HashSet<Class<? extends RpcGateway>> interfaces = new HashSet<>();

		while (clazz != null) {
			for (Class<?> interfaze : clazz.getInterfaces()) {
				if (RpcGateway.class.isAssignableFrom(interfaze)) {
					interfaces.add((Class<? extends RpcGateway>)interfaze);
				}
			}

			clazz = clazz.getSuperclass();
		}

		return interfaces;
	}

	// We don't want this class to be instantiable
	private RpcUtils() {}
}
