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
 * A filter function which combines two filter functions with a logical and. Thus, the filter
 * function only returns true, iff both filters return true.
 *
 * @param <T> Type of the element to filter
 */
public class AndFilterFunction<T> implements FilterFunction<T> {
	private static final long serialVersionUID = -2109562093871155005L;

	private final FilterFunction<T> left;
	private final FilterFunction<T> right;

	public AndFilterFunction(final FilterFunction<T> left, final FilterFunction<T> right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean filter(T value) throws Exception {
		return left.filter(value) && right.filter(value);
	}
}
