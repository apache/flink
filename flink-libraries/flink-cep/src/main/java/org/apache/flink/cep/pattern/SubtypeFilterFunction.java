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

package org.apache.flink.cep.pattern;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * A filter function which filters elements of the given type. A element if filtered out iff it
 * is not assignable to the given subtype of T.
 *
 * @param <T> Type of the elements to be filtered
 */
public class SubtypeFilterFunction<T> implements FilterFunction<T> {
	private static final long serialVersionUID = -2990017519957561355L;

	// subtype to filter for
	private final Class<? extends T> subtype;

	public SubtypeFilterFunction(final Class<? extends T> subtype) {
		this.subtype = subtype;
	}

	@Override
	public boolean filter(T value) throws Exception {
		return subtype.isAssignableFrom(value.getClass());
	}
}
