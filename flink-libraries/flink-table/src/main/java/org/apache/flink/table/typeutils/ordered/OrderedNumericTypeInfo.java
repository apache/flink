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

package org.apache.flink.table.typeutils.ordered;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Arrays;
import java.util.HashSet;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Type information for numeric primitive types: int, long, double, byte, short, float, char.
 */
@Internal
public abstract class OrderedNumericTypeInfo<T> extends OrderedBasicTypeInfo<T> {

	private static final long serialVersionUID = -5937777910658986986L;

	private static final HashSet<Class<?>> numericalTypes = new HashSet<Class<?>>(
		Arrays.asList(
			Integer.class,
			Long.class,
			Double.class,
			Byte.class,
			Short.class,
			Float.class,
			Character.class));

	protected OrderedNumericTypeInfo(Class<T> clazz, TypeSerializer<T> serializer) {
		super(clazz, serializer);

		checkArgument(numericalTypes.contains(clazz),
					"The given class %s is not a numerical type", clazz.getSimpleName());
	}
}
