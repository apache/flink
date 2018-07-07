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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.StateTtlConfiguration;

/**
 * This class wraps folding function with TTL logic.
 *
 * @param <T> Type of the values folded into the state
 * @param <ACC> Type of the value in the state
 *
 * @deprecated use {@link TtlAggregateFunction} instead
 */
@Deprecated
class TtlFoldFunction<T, ACC>
	extends AbstractTtlDecorator<FoldFunction<T, ACC>>
	implements FoldFunction<T, TtlValue<ACC>> {
	private final ACC defaultAccumulator;

	TtlFoldFunction(
		FoldFunction<T, ACC> original, StateTtlConfiguration config, TtlTimeProvider timeProvider, ACC defaultAccumulator) {
		super(original, config, timeProvider);
		this.defaultAccumulator = defaultAccumulator;
	}

	@Override
	public TtlValue<ACC> fold(TtlValue<ACC> accumulator, T value) throws Exception {
		ACC userAcc = getUnexpired(accumulator);
		userAcc = userAcc == null ? defaultAccumulator : userAcc;
		return wrapWithTs(original.fold(userAcc, value));
	}
}
