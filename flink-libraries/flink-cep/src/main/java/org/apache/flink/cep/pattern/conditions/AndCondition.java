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

package org.apache.flink.cep.pattern.conditions;

/**
 * A {@link IterativeCondition condition} which combines two conditions with a logical
 * {@code AND} and returns {@code true} if both are {@code true}.
 *
 * @param <T> Type of the element to filter
 */
public class AndCondition<T> extends IterativeCondition<T> {

	private static final long serialVersionUID = -2471892317390197319L;

	private final IterativeCondition<T> left;
	private final IterativeCondition<T> right;

	public AndCondition(final IterativeCondition<T> left, final IterativeCondition<T> right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean filter(T value, Context<T> ctx) throws Exception {
		return left.filter(value, ctx) && right.filter(value, ctx);
	}

	/**
	 * @return One of the {@link IterativeCondition conditions} combined in this condition.
	 */
	public IterativeCondition<T> getLeft() {
		return left;
	}

	/**
	 * @return One of the {@link IterativeCondition conditions} combined in this condition.
	 */
	public IterativeCondition<T> getRight() {
		return right;
	}
}
