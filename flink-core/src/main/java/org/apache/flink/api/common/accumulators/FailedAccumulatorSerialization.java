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

package org.apache.flink.api.common.accumulators;

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * {@link Accumulator} implementation which indicates a serialization problem with the original
 * accumulator. Accessing any of the {@link Accumulator} method will result in throwing the
 * serialization exception.
 *
 * @param <V> type of the value
 * @param <R> type of the accumulator result
 */
public class FailedAccumulatorSerialization<V, R extends Serializable> implements Accumulator<V, R> {
	private static final long serialVersionUID = 6965908827065879760L;

	private final Throwable throwable;

	public FailedAccumulatorSerialization(Throwable throwable) {
		this.throwable = Preconditions.checkNotNull(throwable);
	}

	public Throwable getThrowable() {
		return throwable;
	}

	@Override
	public void add(V value) {
		ExceptionUtils.rethrow(throwable);
	}

	@Override
	public R getLocalValue() {
		ExceptionUtils.rethrow(throwable);
		return null;
	}

	@Override
	public void resetLocal() {
		ExceptionUtils.rethrow(throwable);
	}

	@Override
	public void merge(Accumulator<V, R> other) {
		ExceptionUtils.rethrow(throwable);
	}

	@Override
	public Accumulator<V, R> clone() {
		ExceptionUtils.rethrow(throwable);
		return null;
	}
}
