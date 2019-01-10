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

import org.apache.flink.annotation.Internal;

/**
 * Utility class containing an {@link IterativeCondition} that always returns
 * {@code true} and one that always returns {@code false}.
 */
@Internal
public class BooleanConditions {

	/**
	 * @return An {@link IterativeCondition} that always returns {@code true}.
	 */
	public static <T> IterativeCondition<T> trueFunction()  {
		return new SimpleCondition<T>() {
			private static final long serialVersionUID = 8379409657655181451L;

			@Override
			public boolean filter(T value) throws Exception {
				return true;
			}
		};
	}

	/**
	 * @return An {@link IterativeCondition} that always returns {@code false}.
	 */
	public static <T> IterativeCondition<T> falseFunction()  {
		return new SimpleCondition<T>() {
			private static final long serialVersionUID = -823981593720949910L;

			@Override
			public boolean filter(T value) throws Exception {
				return false;
			}
		};
	}
}
