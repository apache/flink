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

package org.apache.flink.types;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public enum CustomTypeInfoRegister {

	INSTANCE;

	private final ConcurrentMap<Class<?>, TypeInformation<?>> registeredTypes = new ConcurrentHashMap<>();

	public static CustomTypeInfoRegister getInstance() {
		return INSTANCE;
	}

	public void registerType(Class<?> type, TypeInformation<?> typeInfo) {
		registeredTypes.put(type, typeInfo);
	}

	public void deregisterType(Class<?> type) {
		registeredTypes.entrySet().removeIf(entry -> entry.getKey().equals(type));
	}

	public Optional<TypeInformation> getTypeInfoFor(Class<?> type) {
		return Optional.ofNullable(registeredTypes.get(type));
	}
}
