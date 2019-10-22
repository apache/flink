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

package org.apache.flink.table.runtime.typeutils.coders;

import org.apache.beam.sdk.coders.Coder;

import java.util.Map;

/**
 * The Abstract implementation of coder Finder find coder.
 */
public abstract class AbstractCoderFinder implements CoderFinder {

	@Override
	public <T> Coder<T> findMatchedCoder(Class<T> conversionClass) {
		Coder<T> coder = (Coder<T>) getCoders().get(conversionClass);
		if (coder == null) {
			throw new IllegalArgumentException(
				String.format("Unknown conversion class %s for %s coder.", conversionClass, getDataTypeName()));
		}
		return coder;
	}

	@Override
	public <IN, OUT> Class<OUT> toInternalType(Class<IN> externalType) {
		Class<OUT> internalTypeClass = (Class<OUT>) externalToInterval().get(externalType);
		if (internalTypeClass == null) {
			throw new IllegalArgumentException(
				String.format("Unknown externalType class %s for %s.", externalType, getDataTypeName()));
		}
		return internalTypeClass;
	}

	abstract Map<Class<?>, Coder<?>> getCoders();

	abstract Map<Class<?>, Class<?>> externalToInterval();

	abstract String getDataTypeName();
}
